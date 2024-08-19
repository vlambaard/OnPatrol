import json, time, datetime, queue, re
import os, threading, asyncio, aiofiles, aiohttp
from pathlib import Path
from Common import make_valid_filename, create_mp4, TermToken, \
    create_sqlite3_table, SyncCall, csv2list, aioEvent_ts, generate_code
from ffmpeg import FFmpeg #pypi.org/project/python-ffmpeg/
from calendar import day_name
from deepstack_sdk import ServerConfig, Detection
import logging
logger = logging.getLogger('on_patrol_server')


class DataBaseManager():
    def __init__(self, DBconn):
        self._DBconn = DBconn
        self._lock = asyncio.Lock()
        
    async def _execute(self, stmt, args):
        async with self._lock:
            result = await SyncCall(self._DBconn.execute, None, stmt, args)
        return result        
    
    async def add_camera_log(self, camera_id, event_type, event_time, ipc_name, ipc_sn, channel_name, channel_number):
        stmt = 'INSERT INTO camera_log (CAMERA_ID, EVENT_TYPE, EVENT_TIME, IPC_NAME, IPC_SN, CHANNEL_NAME, CHANNEL_NUMBER) VALUES (?,?,?,?,?,?,?)'
        args = (camera_id, event_type, event_time, ipc_name, ipc_sn, channel_name, channel_number,)
        return await self._execute(stmt, args)

    async def add_image(self, filename, time, log_guid=''):
        if str(filename).strip() == '':
            return       
        stmt = 'INSERT INTO images (FILENAME, TIME, LOG_GUID) VALUES (?,?,?)'
        args = (str(filename), int(time), log_guid,)
        return await self._execute(stmt, args)

    async def get_images_older_than(self, exp_time):
        stmt = 'SELECT * FROM images where time <= (?)'
        args = (exp_time,)
        return await self._execute(stmt,args)
    
    async def delete_image(self, filename):
        if str(filename).strip() == '':
            return           
        stmt = "DELETE FROM images WHERE filename = (?)"
        args = (filename,)
        await self._execute(stmt, args) 

    async def delete_image_guid(self, guid):         
        stmt = "DELETE FROM images WHERE guid = (?)"
        args = (guid,)
        await self._execute(stmt, args) 

    async def setup_tables(self):  
        # create_sqlite3_table() automatically adds a GUID primary key column
        # create camera log table
        columns = [['CAMERA_ID'     , 'integer'],
                   ['EVENT_TYPE'    , 'text'],
                   ['EVENT_TIME'    , 'integer'],
                   ['IPC_NAME'      , 'text'],
                   ['IPC_SN'        , 'text'],
                   ['CHANNEL_NAME'  , 'text'],
                   ['CHANNEL_NUMBER', 'text']]
        indexs = ['CAMERA_ID', 'EVENT_TYPE', 'EVENT_TIME', 'IPC_NAME','IPC_SN' ,'CHANNEL_NUMBER']
        await SyncCall(create_sqlite3_table, None, self._DBconn, 'camera_log', columns, indexs)
        # Create images table
        columns = [['FILENAME', 'text'], 
                   ['TIME'    , 'integer'], 
                   ['LOG_GUID', 'integer']]
        indexs  = ['FILENAME' , 'TIME', 'LOG_GUID']
        await SyncCall(create_sqlite3_table, None, self._DBconn, 'images', columns, indexs)
        logger.debug('[Recorder]         Tables set up')


class NotificationRecorder(threading.Thread):
    def __init__(self, incoming_queue, outgoing_queues, db_conn, config):    
        threading.Thread.__init__(self)
        self.name = 'NotificationRecorder'
        self.incoming_queue = incoming_queue
        self.outgoing_queues = outgoing_queues #A dict {'queuename':queue_obj}
        self.config = config
        self._db_conn = db_conn
        self.dbm = None
        self.exit_flag = None
        self.loop = None
        
    def run(self):
        asyncio.run(self.NotificationRecorderMain())
        logger.debug('[Recorder]         Started')
    
    def stop(self):
        self.exit_flag.set()
        self.incoming_queue.put(TermToken()) #Allow queues to flush through
        self.join()
        logger.debug('[Recorder]         Stopped')


    async def NotificationRecorderMain(self):
        try:
            self.loop=asyncio.get_running_loop()
        except:
            self.loop=asyncio.new_event_loop()
        
        self.exit_flag = aioEvent_ts()
        self.dbm = DataBaseManager(self._db_conn)
        await self.dbm.setup_tables()
    
        self.loop.create_task(self.ImagesDiskCleanUpWorker())
        self.loop.create_task(self.NotificationRecorderScheduler())    
    
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*tasks)
    
    
    async def ImagesDiskCleanUpWorker(self):
        while not self.exit_flag.is_set():
            try:
                expire_time = time.time()-self.config['RECORDER']['IMAGES_KEEP_TIME']
                images = await self.dbm.get_images_older_than(expire_time)
                for image in images:
                    if self.exit_flag.is_set():
                        break
                    try:
                        full_path = os.path.join(self.config['PATHS']['IMAGES_SAVE_PATH'], image['FILENAME'])
                        if await aiofiles.os.path.exists(full_path):
                            await aiofiles.os.remove(full_path)
                    except:
                        pass
                    finally:
                        try:
                            await self.dbm.delete_image_guid(image['GUID'])
                        except:
                            pass
            except Exception as ex:
                logger.error(ex, exc_info=True)
            try:
                await asyncio.wait_for(self.exit_flag.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            
    async def NotificationRecorderScheduler(self):
        while not self.exit_flag.is_set():
            try:
                notification = await SyncCall(self.incoming_queue.get, None)
                if isinstance(notification, TermToken):
                    for outgoing_queue in self.outgoing_queues.values():
                        await SyncCall(outgoing_queue.put,None,notification)
                    break
                self.loop.create_task(self.NotificationRecorderWorker(notification))
            except Exception as ex:
                logger.error(f'[Recorder]         {str(ex)}', exc_info=True)
        logger.debug('[Recorder]         NotificationRecorderScheduler terminated')
        
    async def NotificationRecorderWorker(self, notification):
        if notification.get('RTSP_RECORDING_ENABLED', False):
            file_subpath = await self.generate_file_subpath(notification['CAMERA_NAME'],
                                                            notification['CHANNEL_NUMBER'],
                                                            notification["EVENT_TIME"],
                                                            self.config['PATHS']['IMAGES_SAVE_PATH'],
                                                            '.mp4')
            file_fullpath = os.path.join(self.config['PATHS']['IMAGES_SAVE_PATH'], file_subpath)
            if notification['DEEPSTACK_ENABLED']:
                width = '1280'
            else:
                width = '640'        
            try:
                Path(file_fullpath).touch()
                logger.debug(f'[Recorder]         {notification["CAMERA_NAME"]} Capturing RTSP feed')
                await self.save_rtsp(url    = notification['RTSP_FULL_URL'], 
                                     output = file_fullpath, 
                                     length = notification['RTSP_RECORDING_LENGTH_SEC'], 
                                     width  = width, 
                                     height = '-1', 
                                     fps    = '10')
            except Exception:
                logger.error(f'[Recorder]         {notification["CAMERA_NAME"]} RTSP failed.',exc_info=True)
            else:
                notification['MEDIA_FILENAMES'] = [file_fullpath]
                log_guid = await self.dbm.add_camera_log(camera_id      = notification['CAMERA_ID'],
                                                         event_type     = json.dumps(notification['EVENT_TYPE']),
                                                         event_time     = notification['EVENT_TIME'].timestamp(),
                                                         ipc_name       = notification['CAMERA_NAME'],
                                                         ipc_sn         = notification['IPC_SN'],
                                                         channel_name   = notification['CHANNEL_NAME'],
                                                         channel_number = notification['CHANNEL_NUMBER'])
                await self.dbm.add_image(filename = file_subpath, 
                                         time     = time.time(), 
                                         log_guid = log_guid) 
            
                if notification.get('DEEPSTACK_ENABLED', False):
                    if notification['DEEPSTACK_PREFILTER_ENABLED']:
                       min_confidence = self.get_deepstack_filter_profile(notification)
                    else:
                        min_confidence = notification['DEEPSTACK_MIN_CONFIDENCE']
                    if min_confidence:
                        detections = await SyncCall(self.DeepStackDetection, 
                                                    None, 
                                                    min_confidence = min_confidence, 
                                                    videos         = [file_fullpath])
                        notification['EVENT_TYPE'].extend(detections)
                        logger.debug(f'[Recorder]         {notification["CAMERA_NAME"]} Objects detected: {detections}')
                    try:
                        await self.resize_video_file(file_fullpath, width='640')
                    except Exception as ex:
                        logger.error(f'[Recorder]         {notification["CAMERA_NAME"]} resize_video_file failed. {str(ex)}')
                del notification['IMAGES']
                del notification['VIDEOS']
                for outgoing_queue in self.outgoing_queues.values():
                    await SyncCall(outgoing_queue.put,None,notification)
                return
    
        if notification['VIDEOS']:
            pass
        
        if notification['IMAGES']:
            if notification.get('DEEPSTACK_ENABLED', False):
                if notification['DEEPSTACK_PREFILTER_ENABLED']:
                   min_confidence = self.get_deepstack_filter_profile(notification)
                else:
                    min_confidence = notification['DEEPSTACK_MIN_CONFIDENCE']
                if min_confidence:
                    detections = await SyncCall(self.DeepStackDetection,
                                                None,
                                                min_confidence = min_confidence, 
                                                images = [img['payload'] for img in notification['IMAGES']])
                    notification['EVENT_TYPE'].extend(detections)
                    logger.debug(f'[Recorder]         {notification["CAMERA_NAME"]} Objects detected: {detections}')
                    
            if len(notification['IMAGES']) > 1:
                file_subpath = await self.generate_file_subpath(camera_name      = notification['CAMERA_NAME'],
                                                                channel_number   = notification['CHANNEL_NUMBER'],
                                                                event_time       = notification["EVENT_TIME"],
                                                                images_save_path = self.config['PATHS']['IMAGES_SAVE_PATH'],
                                                                file_extention   = '.mp4')
                file_fullpath = os.path.join(self.config['PATHS']['IMAGES_SAVE_PATH'], file_subpath)
                try:
                    Path(file_fullpath).touch()
                    await SyncCall(create_mp4, 
                                   None, 
                                   images        = [img['payload'] for img in notification['IMAGES']], 
                                   framerate     = 1.25, 
                                   out_file_path = file_fullpath)
                except Exception as ex:
                    logger.error(f'[Recorder]         {str(notification["CAMERA_NAME"])}: create_mp4 from still images failed. {str(ex)}')
                else:
                    notification['MEDIA_FILENAMES'] = [file_fullpath]
                    log_guid = await self.dbm.add_camera_log(camera_id      = notification['CAMERA_ID'],
                                                             event_type     = json.dumps(notification['EVENT_TYPE']),
                                                             event_time     = notification['EVENT_TIME'].timestamp(),
                                                             ipc_name       = notification['CAMERA_NAME'],
                                                             ipc_sn         = notification['IPC_SN'],
                                                             channel_name   = notification['CHANNEL_NAME'],
                                                             channel_number = notification['CHANNEL_NUMBER'])
                    await self.dbm.add_image(filename = file_subpath, 
                                             time     = time.time(), 
                                             log_guid = log_guid)
                finally:
                    del notification['IMAGES']
                    del notification['VIDEOS']
                    for outgoing_queue in self.outgoing_queues.values():
                        await SyncCall(outgoing_queue.put,None,notification)
                    return
            
            elif len(notification['IMAGES']) == 1:
                file_subpath = await self.generate_file_subpath(camera_name = notification['CAMERA_NAME'],
                                                                channel_number = notification['CHANNEL_NUMBER'],
                                                                event_time = notification["EVENT_TIME"],
                                                                images_save_path = self.config['PATHS']['IMAGES_SAVE_PATH'],
                                                                file_extention = notification['IMAGES'][0]['type'])
                file_fullpath = os.path.join(self.config['PATHS']['IMAGES_SAVE_PATH'], file_subpath)
                try:
                    Path(file_fullpath).touch()
                    async with aiofiles.open(file_fullpath, 'wb') as out:
                            await out.write(notification['IMAGES'][0]['payload'])
                            await out.flush()
                except Exception as ex:
                    logger.error(f'[Recorder]         notification["CAMERA_NAME"] Failed to same image. {str(ex)}')
                else:
                    notification['MEDIA_FILENAMES'] = [file_fullpath]
                    log_guid = await self.dbm.add_camera_log(camera_id      = notification['CAMERA_ID'],
                                                             event_type     = json.dumps(notification['EVENT_TYPE']),
                                                             event_time     = notification['EVENT_TIME'].timestamp(),
                                                             ipc_name       = notification['CAMERA_NAME'],
                                                             ipc_sn         = notification['IPC_SN'],
                                                             channel_name   = notification['CHANNEL_NAME'],
                                                             channel_number = notification['CHANNEL_NUMBER'])
                    await self.dbm.add_image(filename = file_subpath, 
                                             time     = time.time(), 
                                             log_guid = log_guid)
                finally:
                    del notification['IMAGES']
                    del notification['VIDEOS']
                    for outgoing_queue in self.outgoing_queues.values():
                        await SyncCall(outgoing_queue.put,None,notification)
                    return
        return
    
    
    def DeepStackDetection(self, min_confidence=0.5, images=[], videos=[]):
        detections = []
        deepstack_srv = Detection(ServerConfig(server_url = self.config['DEEPSTACK']['URL'],
                                               api_key    = self.config['DEEPSTACK']['API_KEY']))

        for image in images:
            try:
                response = deepstack_srv.detectObject(image           = image,
                                                      min_confidence  = min_confidence,
                                                      output          = None)
            except Exception as ex:
                logger.exception(ex)
            else:
                detections.extend([obj.label for obj in response.detections])
        
        for video in videos:
            try:
                response = deepstack_srv.detectObjectVideo(video             = video,
                                                           min_confidence    = min_confidence,
                                                           output            = None,
                                                           continue_on_error = True)
            except Exception as ex:
                logger.exception(ex)
            else:
                detections.extend([obj.label for obj in sum([x.detections for x in response.values()], []) ])
        
        # Make list of unique objects
        detections = [obj.lower() for obj in set(detections)]
        return detections
    
    
    async def save_rtsp(self, url, output, length = '4', width='-1', height='-1', fps='10'):
        ffmpeg = (FFmpeg()
                    .option('y')
                    .option('an')  
                    .input(str(url),
                           rtsp_transport='tcp',
                           timeout='5000000',
                           rtsp_flags='prefer_tcp',
                           t=str(length)                      
                           )
                    .output(str(output),
                            {'c:v' : 'libx264'},
                            r = str(fps),
                            vf= f'scale={str(width)}:{str(height)}'))
        
        #try:
        await ffmpeg.execute()
        #except Exception as ex:
        #    logger.exception(ex)

            
    def get_deepstack_filter_profile(self, notification):
        # Check for individual camera name rules
        if notification['CAMERA_NAME'].lower() in self.config['DEEPSTACK']['CAMERA_NAME_INDEX'].keys():
            profile_keys = self.config['DEEPSTACK']['CAMERA_NAME_INDEX'][notification['CAMERA_NAME'].lower()]
            logger.debug(f'[Recorder]         {str(notification["CAMERA_NAME"])}: using profile {str(profile_keys)}')
            for key in profile_keys:
                profile = self.config['DEEPSTACK']['CAMERA_PROFILES'][key]
                #Check channel names
                if profile['CHANNEL_NAMES']:
                    if notification['CHANNEL_NAME'].lower() not in profile['CHANNEL_NAMES']:
                        continue
                #Check channel numbers
                if profile['CHANNEL_NUMBERS']:
                    if notification['CHANNEL_NUMBER'].lower() not in profile['CHANNEL_NUMBERS']:
                        continue
                #Check weekday
                if not profile[day_name[notification['EVENT_TIME'].weekday()].upper()]:
                    continue
                #Check time slot
                if profile['TIME_START'] > profile['TIME_STOP']:
                    if notification['EVENT_TIME'].time() < profile['TIME_STOP'] or notification['EVENT_TIME'].time() >= profile['TIME_START']:
                        return profile['MIN_CONFIDENCE']
                elif profile['TIME_START'] < profile['TIME_STOP']:
                    if notification['EVENT_TIME'].time() < profile['TIME_STOP'] and notification['EVENT_TIME'].time() >= profile['TIME_START']:
                        return profile['MIN_CONFIDENCE']
                else:
                    return profile['MIN_CONFIDENCE']
    
        # Check for rules that apply to all camera (wildcard *)
        #   Note: keys in DEEPSTACK_ALL_CAMERAS_INDEX are also the keys in 
        #   DEEPSTACK_CAMERA_PROFILES. The values in DEEPSTACK_ALL_CAMERAS_INDEX
        #   are a list of camera names to ignore.
        for key in self.config['DEEPSTACK']['ALL_CAMERAS_INDEX'].keys():
            #Check if camera name is excluded from wildcard
            if notification['CAMERA_NAME'].lower() in self.config['DEEPSTACK']['ALL_CAMERAS_INDEX'][key]:
                continue
            
            profile = self.config['DEEPSTACK']['CAMERA_PROFILES'][key]
            #Check channel names
            if profile['CHANNEL_NAMES']:
                if notification['CHANNEL_NAME'].lower() not in profile['CHANNEL_NAMES']:
                    continue
            #Check channel numbers
            if profile['CHANNEL_NUMBERS']:
                if notification['CHANNEL_NUMBER'].lower() not in profile['CHANNEL_NUMBERS']:
                    continue
            #Check weekday
            if not profile[day_name[notification['EVENT_TIME'].weekday()].upper()]:
                continue
            #Check time slot
            if profile['TIME_START'] > profile['TIME_STOP']:
                if notification['EVENT_TIME'].time() < profile['TIME_STOP'] or notification['EVENT_TIME'].time() >= profile['TIME_START']:
                    logger.debug('[Recorder]         '+str(notification['CAMERA_NAME']) + ': using Deepstack profile ' + str(key))
                    return profile['MIN_CONFIDENCE']
            elif profile['TIME_START'] < profile['TIME_STOP']:
                if notification['EVENT_TIME'].time() < profile['TIME_STOP'] and notification['EVENT_TIME'].time() >= profile['TIME_START']:
                    logger.debug('[Recorder]         '+str(notification['CAMERA_NAME']) + ': using Deepstack profile ' + str(key))
                    return profile['MIN_CONFIDENCE']
            else:
                logger.debug('[Recorder]         '+str(notification['CAMERA_NAME']) + ': using Deepstack profile ' + str(key))
                return profile['MIN_CONFIDENCE']
            
        return None        
            
    
    async def generate_file_subpath(self, camera_name, channel_number, event_time, images_save_path, file_extention, count=None):
        filename = make_valid_filename(event_time.strftime("%Y-%m-%dT%H-%M-%S")+'_'+generate_code(5))
        sub_dir = make_valid_filename(camera_name + '_Ch' + str(channel_number))    
        save_path = os.path.join(images_save_path, sub_dir)
        if not await aiofiles.os.path.isdir(save_path):
            try:
                await aiofiles.os.mkdir(save_path)
            except Exception as dr_ex:
                logger.error(dr_ex, exc_info=True)
        if count:
            path = os.path.join(sub_dir, filename+f'({str(count)})'+file_extention)
        else:
            path = os.path.join(sub_dir, filename+file_extention)
        return path
        
    async def resize_video_file(self, file_fullpath, width='640', height='-1'):
        temp_file = os.path.splitext(file_fullpath)[0]+'_temp.mp4'
        ffmpeg = (FFmpeg()
                    .option('y')
                    .option('an')
                    .input(file_fullpath)
                    .output(temp_file,
                            vf= f'scale={str(width)}:{str(height)}'))
        # @ffmpeg.on('start')
        # def on_start(arguments):
        #     logger.debug(f'ffmpeg arguments: {arguments}')
        try:
            await ffmpeg.execute()
        except Exception as ex:
            logger.exception(ex)
        else:
            try:
                await aiofiles.os.remove(file_fullpath)
                await aiofiles.os.rename(temp_file, file_fullpath)
            except Exception as ex2:
                logger.exception(ex2)
                
        