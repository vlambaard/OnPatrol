from email import message_from_bytes, message_from_string
import queue
from aiosmtpd.controller import Controller as SMTPController
import html2text
import encodings.idna #Keep import, fixes an idna error that sometimes happen
import datetime, re
from dateutil.parser import parse as datetime_parser
from Common import xstr, get_ctype_file_extension, SyncCall, csv2list
import os
import logging
logger = logging.getLogger('on_patrol_server')


class SMTP_Controller_Handler:
    '''
    SMTP_Controller_Handler() handles incoming messages to the SMTP server.
      -> A message is only processed if addressed to "localbot@localhost.local" 
         and the subject of the email is "LPR_ALERT". Anything else is ignored.
         These default values can be set in the User Config at the top of the 
         file.
      -> If the message body contains the expected XML template, the content
         is parsed and alert details are stored as a new entry in the alert DB.
      -> Any attached images are saved to file and the file names are recorded 
         in the alert DB.
    '''
    
    
    def __init__(self, message_class=None, OutgoingQueues={}, Config={}):
        self.message_class = message_class
        self.OutgoingQueues = OutgoingQueues
        self.config = Config
        

    def AddNotificationQueue(self, Name, Queue):
        self.OutgoingQueues.update({Name:Queue})
        

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):
        if address not in self.config['CAMERAS']['EMAIL_INDEX'].keys():
            if not self.config['UNREGISTERED_CAMERAS']['EMAIL_FROM_UNREGISTERED_CAMERAS_ENABLED']:
                logger.debug(f'[EmailServer]      {str(address)} : Unregistered email senders not allowed: {address}')
                return '550 permission denied'
        envelope.rcpt_tos.append(address)
        return '250 OK'

    async def handle_DATA(self, server, session, envelope):
        notification_item = None
        envelope = self.prepare_message(session, envelope)
        email_address = envelope['X-RcptTo'].strip().lower()
        if email_address in self.config['CAMERAS']['EMAIL_INDEX'].keys():
            template_key  = self.config['CAMERAS']['EMAIL_INDEX'][email_address]['EMAIL_TEMPLATE']
            logger.debug(f'[EmailServer]      {email_address} : parsing registered email with email template: {template_key}')
            parsed_msg = self.parse_message(envelope, template_key)
            if parsed_msg:
                camera_config_key = self.config['CAMERAS']['EMAIL_INDEX'][email_address]['CHANNEL'].get(parsed_msg['CHANNEL_NUMBER'],None)
                if camera_config_key:
                    notification_item = self.handle_camera_in_config(camera_config_key, parsed_msg)
                else:
                    logger.error(f'[EmailServer]      {email_address} : Could not identify camera in config, unkown channel number')
        elif email_address in self.config['UNREGISTERED_CAMERAS']['EMAIL_INDEX'].keys():
            template_key = self.config['UNREGISTERED_CAMERAS']['EMAIL_INDEX'][email_address]
            logger.debug(f'[EmailServer]      {email_address} : parsing unregistered camera with email template: {template_key}')
            parsed_msg = self.parse_message(envelope, template_key)
            if parsed_msg:
                notification_item = self.handle_unregistered_camera(parsed_msg)           
        else:
            template_key = 'Not found or camera disabled'

        if notification_item is None:
            logger.debug(f'[EmailServer]      {email_address} : FAILED to parse email with template: {template_key}')
            return '550 permission denied'        
        else:
            try:
                for q in self.OutgoingQueues.values():
                    await SyncCall(q.put, None, notification_item)        
            except queue.Full:
                return '421 incoming queue full, try again later'

        return '250 OK'

    def prepare_message(self, session, envelope):
        # If the server was created with decode_data True, then data will be a
        # str, otherwise it will be bytes.
        data = envelope.content
        if isinstance(data, bytes):
            message = message_from_bytes(data, self.message_class)
        else:
            assert isinstance(data, str), (
              'Expected str or bytes, got {}'.format(type(data)))
            message = message_from_string(data, self.message_class)
        message['X-Peer'] = xstr(session.peer)
        message['X-MailFrom'] = envelope.mail_from
        message['X-RcptTo'] = ', '.join(envelope.rcpt_tos)
        return message
    
    def handle_unregistered_camera(self, parsed_msg):
        parsed_msg.update({'CAMERA_ID'  : None})
        parsed_msg.update({'CAMERA_NAME': parsed_msg['IPC_NAME']})
        deepstack_enabled = bool(self.config['DEEPSTACK']['ENABLED'] and self.config['UNREGISTERED_CAMERAS']['DEEPSTACK_DETECTION_ENABLED'])
        if deepstack_enabled:
            parsed_msg.update({'DEEPSTACK_ENABLED':deepstack_enabled})
            parsed_msg.update({'DEEPSTACK_MIN_CONFIDENCE': self.config['UNREGISTERED_CAMERAS']['DEEPSTACK_MIN_CONFIDENCE']})
            parsed_msg.update({'DEEPSTACK_PREFILTER_ENABLED': self.config['UNREGISTERED_CAMERAS']['DEEPSTACK_PREFILTER_ENABLED']})
        return parsed_msg
    
    def handle_camera_in_config(self, camera_config_key, parsed_msg):
        camera_config = self.config['CAMERAS']['CONFIGS'].get(camera_config_key, None)
        if camera_config is None:
            return None
        
        parsed_msg.update({'CAMERA_ID'    : camera_config_key})
        parsed_msg.update({'CAMERA_NAME'  : camera_config['CAMERA_NAME']})
        parsed_msg.update({'CHANNEL_NAME' : camera_config['CAMERA_NAME']})
        
        if camera_config['RTSP_RECORDING_ENABLED']:
            if any(x in parsed_msg['EVENT_TYPE'] for x in camera_config['RTSP_REC_ON_EVENT_TYPE']) or \
                '*' in camera_config['RTSP_REC_ON_EVENT_TYPE'] or camera_config['RTSP_REC_ON_EVENT_TYPE'] == []:
                parsed_msg.update({'RTSP_RECORDING_ENABLED':camera_config['RTSP_RECORDING_ENABLED']})
                parsed_msg.update({'RTSP_FULL_URL':camera_config['RTSP_FULL_URL']})
                parsed_msg.update({'RTSP_RECORDING_LENGTH_SEC':camera_config['RTSP_RECORDING_LENGTH_SEC']})
        
        deepstack_enabled = bool(camera_config['DEEPSTACK_DETECTION_ENABLED'] and self.config['DEEPSTACK']['ENABLED'])
        if deepstack_enabled:
            parsed_msg.update({'DEEPSTACK_ENABLED':deepstack_enabled})
            parsed_msg.update({'DEEPSTACK_MIN_CONFIDENCE':camera_config['DEEPSTACK_MIN_CONFIDENCE']})
            parsed_msg.update({'DEEPSTACK_PREFILTER_ENABLED':camera_config['DEEPSTACK_PREFILTER_ENABLED']})
            
        return parsed_msg
    
    def parse_message(self, Message, template_key):
        template = self.config['SMTP']['EMAIL_TEMPLATES'].get(template_key.lower(), None)
        if template is None:
            logger.error(f'[EmailServer]      {Message["X-RcptTo"]} : Cannot parse email, invalid email template key: {template_key}')
            return None
        
        #Return the email body text and images
        [msgtext,images,videos] = self.get_content_from_message(Message)

        #Check if test email
        if template['TEST_MESSAGE_RE'] != '':
            re_sult = re.search(template['TEST_MESSAGE_RE'], msgtext)
        else:
            re_sult = None
            
        if re_sult is not None:
            if template['TEST_MESSAGE_CAMERA_NAME_RE']:
                try:
                    re_sult = re.search(template['TEST_MESSAGE_CAMERA_NAME_RE'],msgtext)
                    IPC_NAME = re_sult[template['TEST_MESSAGE_CAMERA_NAME_GROUP']].strip() if re_sult is not None else ''
                except:
                    IPC_NAME = 'unkown_camera'
            else:
                IPC_NAME = 'unkown_camera'
            EVENT_TYPE = ['Test Notification.']
            EVENT_TIME = datetime.datetime.now()#.strftime('%Y-%m-%dT%H:%M:%S')
            IPC_SN = ''
            CHANNEL_NAME = 'test'
            CHANNEL_NUMBER = '1'
        else:
            if template['EVENT_TYPE_RE']:
                try:
                    re_sult = re.search(template['EVENT_TYPE_RE'],msgtext)
                    EVENT_TYPE = csv2list(re_sult[template['EVENT_TYPE_GROUP']].strip() if re_sult is not None else '')
                except:
                    EVENT_TYPE = ['']
            else:
                EVENT_TYPE = ['']
            
            if template['EVENT_DATETIME_RE']:
                try:
                    re_sult = re.search(template['EVENT_DATETIME_RE'],msgtext)
                    EVENT_TIME = datetime_parser(re_sult[template['EVENT_DATE_GROUP']]+'T'+re_sult[template['EVENT_TIME_GROUP']]) if re_sult is not None else datetime.datetime.now()
                except:
                    EVENT_TIME =  datetime.datetime.now()
            else:
                EVENT_TIME =  datetime.datetime.now()
            
            if template['CAMERA_NAME_RE']:
                try:
                    re_sult = re.search(template['CAMERA_NAME_RE'],msgtext)
                    IPC_NAME = re_sult[template['CAMERA_NAME_GROUP']].strip() if re_sult is not None else 'unknown_camera'
                except:
                    IPC_NAME = 'unknown_camera'
            else:
                IPC_NAME = 'unknown_camera'
            
            if template['SERIAL_NUMBER_RE']:
                try:
                    re_sult = re.search(template['SERIAL_NUMBER_RE'],msgtext)
                    IPC_SN = re_sult[template['SERIAL_NUMBER_GROUP']].strip() if re_sult is not None else ''
                except:
                    IPC_SN = ''
            else:
                IPC_SN = ''
            
            if template['CHANNEL_NAME_RE']:
                try:
                    re_sult = re.search(template['CHANNEL_NAME_RE'],msgtext)
                    CHANNEL_NAME = re_sult[template['CHANNEL_NAME_GROUP']].strip() if re_sult is not None else IPC_NAME
                except:
                    CHANNEL_NAME = IPC_NAME
            else:
                CHANNEL_NAME = IPC_NAME
            
            if template['CHANNEL_NUMBER_RE']:
                try:
                    re_sult = re.search(template['CHANNEL_NUMBER_RE'],msgtext)            
                    CHANNEL_NUMBER = re_sult[template['CHANNEL_NUMBER_GROUP']].strip() if re_sult is not None else '1'
                except:
                    CHANNEL_NUMBER = '1'
            else:
                CHANNEL_NUMBER = '1'

                   
        logger.info(f' [EmailServer]      {Message["X-RcptTo"]} : {IPC_NAME} CH({CHANNEL_NUMBER}):"{CHANNEL_NAME}", EVENT:{EVENT_TYPE}, Image(s):{str(len(images))}')
        return {'EVENT_TYPE'    :EVENT_TYPE,
                'EVENT_TIME'    :EVENT_TIME,
                'IPC_NAME'      :IPC_NAME,
                'IPC_SN'        :IPC_SN,
                'CHANNEL_NAME'  :CHANNEL_NAME, 
                'CHANNEL_NUMBER':CHANNEL_NUMBER, 
                'IMAGES'        :images,
                'VIDEOS'        :videos
                } 

    def get_content_from_message(self, message):
        '''Extract the text body and attachments from the email'''
        text_parts = []
        images = []
        videos = []
        for part in message.walk():
            ctype = part.get_content_type()
            if ctype == 'text/plain':
                text_parts.append( part.get_payload(decode=True).decode('utf-8') ) #decode and utf-8 must be there!
            elif ctype == 'text/html':
                text_parts.append( html2text.html2text( part.get_payload(decode=True).decode('utf-8') ) )
            elif ctype.lower().startswith('image/'):
                part_filename = part.get_filename()
                if part_filename:
                    ext = os.path.splitext(part_filename)[1]
                else:
                    ext = ''
                if not ext:
                    ext = get_ctype_file_extension(ctype)
                images.append({'type':ext, 'payload':part.get_payload(decode=True)})
            elif ctype.lower().startswith('application/'):
                part_filename = part.get_filename()
                if part_filename:
                    ext = os.path.splitext(part_filename)[1]
                else:
                    ext = ''
                if str(ext).lower() in ['.jpg', '.jpeg', '.png','.bmp', '.gif']:
                    images.append({'type':str(ext), 'payload':part.get_payload(decode=True)})
                elif str(ext).lower() in ['.mp4', '.avi']:
                    videos.append({'type':str(ext), 'payload':part.get_payload(decode=True)})
                else:
                    logger.debug(f'[EmailServer]      Unknown {ctype} email attachment: {ext}')
            # else:
            #     logger.debug(f'[EmailServer] Unknown email attachment: {ctype}')
        return [''.join(text_parts), images, videos]


def SMTPServer(Hostname, Port, OutgoingQueues, Config):
    server = SMTPController(SMTP_Controller_Handler(OutgoingQueues     = OutgoingQueues,
                                                    Config             = Config), 
                            hostname = Hostname, 
                            port     = Port)
    logger.info(f' [EmailServer]      SMTP started on {Hostname}:{str(Port)}')                             
    return server
    