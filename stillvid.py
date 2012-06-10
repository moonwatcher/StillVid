#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import re
import logging
import json
import math
from StringIO import StringIO
from datetime import datetime
from datetime import timedelta
from argparse import ArgumentParser
from subprocess import Popen, PIPE

HANDBRAKE = '/opt/local/bin/HandBrakeCLI'
log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}
expression = {
    'file name':{
        'pattern':re.compile(ur'^(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})-(?P<hour>[0-9]{2})-(?P<minute>[0-9]{2})-(?P<second>[0-9]{2})-(?P<microsecond>[0-9]{2})\.(?P<kind>jpg)$'),
        'iso format':'{year}-{month}-{day}T{hour}:{minute}:{second}.{microsecond}'
    },
    'time delta':{
        'pattern':re.compile('(?:(?P<hours>[0-9]+)h)?(?:(?P<minutes>[0-9]+)m)?(?:(?P<seconds>[0-9]+)s)?'),
    }
}
config = {
    'profile':{
        'default':{
            'pack':{
                'frame per second':8,
                'codec':'huffyuv',
            },
            'transcode':{
                '--loose-anamorphic':None,
                '--quality':'20',
                '--encoder':'x264',
                '--encopts':'mixed-refs=1:ref=3:bframes=3:me=umh:b-adapt=2:trellis=0:b-pyramid=none:subme=9:vbv-maxrate=5500:vbv-bufsize=5500:cabac=1',
            },
        },
        'hourly':{
            'pack':{
                'frame per second':8,
                'codec':'huffyuv',
            },
            'transcode':{
                '--loose-anamorphic':None,
                '--quality':'20',
                '--encoder':'x264',
                '--encopts':'mixed-refs=1:ref=3:bframes=3:me=umh:b-adapt=2:trellis=0:b-pyramid=none:subme=9:vbv-maxrate=5500:vbv-bufsize=5500:cabac=1',
                '--maxWidth':'480',
            },
        },
        'daily':{
            'pack':{
                'frame per second':8,
                'codec':'huffyuv',
            },
            'transcode':{
                '--loose-anamorphic':None,
                '--quality':'18',
                '--encoder':'x264',
                '--encopts':'mixed-refs=1:ref=3:bframes=3:me=umh:b-adapt=2:trellis=0:b-pyramid=none:subme=9:vbv-maxrate=5500:vbv-bufsize=5500:cabac=1',
            },
        },
    },
    'camera':{
        'nixon':{
            'location':{
                'watch directory':'/home/nixon/camera/watch',
                'buffer directory':'/home/nixon/camera/buffer',
                'temp directory':'/home/nixon/camera/temp',
                'product directory':'/media/purple/camera/product',
                'json':'/home/nixon/camera/db/db.json',
            },
            'default':{
                'profile':'default',
            },
        }
    }
}

class SurveyScraper(object):
    def __init__(self, env, config):
        self.log = logging.getLogger('scraper')
        self.env = env
        self.node = None
        self.config = config
        self.volatile = False
    
    @property
    def profile(self):
         return config['profile'][self.env['profile']]
    
    @property
    def json(self):
         return json.dumps(self.node, ensure_ascii=False, sort_keys=True, indent=4,  default=default_json_handler).encode('utf-8')
    
    def load(self):
        if 'json' in self.config['location']:
            if os.path.exists(self.config['location']['json']):
                path = os.path.abspath(self.config['location']['json'])
                try:
                    fnode = open(path, 'r')
                    stream = StringIO(fnode.read())
                    fnode.close()
                except IOError as ioerr:
                    self.log.warning(u'Failed to load config file %s', path)
                    self.log.debug(ioerr)
                else:
                    try:
                        document = json.load(stream)
                    except ValueError, e:
                        self.log.warning(u'Failed to decode JSON document %s', path)
                        self.log.debug(u'Exception raised %s', unicode(e))
                    else:
                        self.node = document
                        for frame in self.node['frame']:
                            frame['timestamp'] = datetime.strptime(frame['timecode'], "%Y-%m-%dT%H:%M:%S.%f")
            else:
                # in the event that the buffer index is missing
                # recreate it by scanning the buffer directory
                self.log.info(u'No index found, reindexing...')
                self.node = { 'frame':[], }
                for path in os.listdir(self.config['location']['buffer directory']):
                    match = expression['file name']['pattern'].search(path)
                    if match is not None:
                        o = match.groupdict()
                        iso = expression['file name']['iso format'].format(**o)
                        record = {
                            'timestamp':datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%f"),
                            'path':os.path.abspath(os.path.join(self.config['location']['buffer directory'],path)),
                        }
                        record['timecode'] = record['timestamp'].strftime("%Y-%m-%dT%H:%M:%S.%f")
                        self.node['frame'].append(record)
                self.log.info(u'Reindex found %d frames in buffer', len(self.node['frame']))
                self.volatile = True
    
    def unload(self):
        if self.volatile:
            for frame in self.node['frame']:
                del frame['timestamp']
                
            self.log.debug(u'Flushing buffer index with %d frames to %s', len(self.node['frame']), self.config['location']['json'])
            self.node['modified'] = datetime.now()
            path = os.path.abspath(self.config['location']['json'])
            if self.varify_directory(os.path.dirname(path)):
                try:
                    fconf = open(path, 'w')
                    fconf.write(self.json)
                    fconf.close()
                except IOError as error:
                    self.log.error(str(error))
                else:
                    self.volatile = False
    
    def commit(self):
        if os.path.isdir(self.config['location']['watch directory']):
            batch = []
            for path in os.listdir(self.config['location']['watch directory']):
                match = expression['file name']['pattern'].search(path)
                if match is not None:
                    o = match.groupdict()
                    iso = expression['file name']['iso format'].format(**o)
                    record = {
                        'timestamp':datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%f"),
                        'path':os.path.abspath(os.path.join(self.config['location']['buffer directory'],path)),
                        'source':os.path.abspath(os.path.join(self.config['location']['watch directory'],path)),
                    }
                    record['timecode'] = record['timestamp'].strftime("%Y-%m-%dT%H:%M:%S.%f")
                    batch.append(record)
                    
            if batch:
                self.log.debug(u'Indexing %d new frames', len(batch))
                for record in batch:
                    # move the frame to the target location
                    proc = Popen(['mv', record['source'], record['path']])
                    proc.communicate()
                    
                    # remove reference to source
                    del record['source']
                    
                    # add a reference of the frame to the index
                    self.node['frame'].append(record)
                    self.volatile = True
                    
    
    def purge(self):
        query = self.select()
        query['batch'] = []
        for frame in self.node['frame']:
            if frame['timestamp'] < query['begin'] or frame['timestamp'] > query['end']:
                #if os.path.isfile(frame['path']): os.remove(frame['path'])
                self.volatile = True
            else:
                query['batch'].append(frame)
                
        self.log.info('Removed all but %d frames in %s from %s to %s', len(query['batch']), str(query['duration']), query['begin'].isoformat(), query['end'].isoformat())
        self.node['frame'] = query['batch']
    
    def select(self):
        node = { 'now':datetime.now(), }
        if 'from timestamp' in self.env:
            node['from timestamp'] = datetime.strptime(self.env['from timestamp'], "%Y-%m-%d %H:%M:%S")
            
        if 'to timestamp' in self.env:
            node['to timestamp'] = datetime.strptime(self.env['to timestamp'], "%Y-%m-%d %H:%M:%S")
            
        if 'timestamp window' in self.env:
            match = expression['time delta']['pattern'].search(self.env['timestamp window'])
            if match is not None:
                o = {}
                for k,v in match.groupdict().iteritems():
                    if v: o[k] = int(v)
                node['timestamp window'] = timedelta(**o)
                
        if 'timestamp offset' in self.env:
            match = expression['time delta']['pattern'].search(self.env['timestamp offset'])
            if match is not None:
                o = {}
                for k,v in match.groupdict().iteritems():
                    if v: o[k] = int(v)
                node['timestamp offset'] = timedelta(**o)
                
        node['end'] = node['now'] - node['timestamp offset']
        node['begin'] = node['end'] - node['timestamp window']
        node['duration']  = node['end'] - node['begin']
        node['name'] = '{}~{}'.format(node['begin'].strftime("%Y-%m-%d-%H-%M-%S"), node['end'].strftime("%Y-%m-%d-%H-%M-%S"))
        return node
    
    def pack(self):
        query = self.select()
        query['batch'] = []
        for frame in self.node['frame']:
            if frame['timestamp'] > query['begin'] and frame['timestamp'] < query['end']:
                query['batch'].append(frame)
        if batch:
            self.log.info('Selecting %d frames in %s from %s to %s', len(query['batch']), str(query['duration']), query['begin'].isoformat(), query['end'].isoformat())
            
            # sort the frames in the batch by timestamp
            query['batch'] = sorted(query['batch'], key=lambda frame: frame['timestamp'])
            query['temp'] = os.path.abspath(os.path.join(self.config['location']['temp directory'],query['name']))
            if self.varify_directory(query['temp']):
                
                # Copy selected frames to temp folder with sequential numbers
                i = 1
                pad = int(math.ceil(math.log(len(batch), 10)))
                for frame in query['batch']:
                    command = [
                        'cp',
                        frame['path'],
                        os.path.join(query['temp'], 'frame{:0{}d}.jpg'.format(i, pad))
                    ]
                    proc = Popen(command)
                    proc.communicate()
                    i += 1
                    
                # pack jpgs into a stream
                uncompressed = '{0}/{1}.mkv'.format(query['temp'], 'video')
                self.log.debug('pack uncompressed sequence to %s', uncompressed)
                command = [
                    'ffmpeg', 
                    '-r', str(self.profile['pack']['frame per second']), 
                    '-i', '{0}/frame%0{1}d.jpg'.format(query['temp'], pad), 
                    '-vcodec', self.profile['pack']['codec'], 
                    uncompressed
                ]
                proc = Popen(command, stderr=PIPE, stdout=PIPE)
                proc.communicate()
                
                # run handbrake to compress the stream
                if self.varify_directory(self.config['location']['product directory']):
                    product = '{0}/{1}.m4v'.format(self.config['location']['product directory'], query['name'])
                    self.log.debug('Compress video to %s', product)
                    command = [ HANDBRAKE ]
                    for k,v in self.profile['transcode'].iteritems():
                        if k: command.append(k)
                        if v: command.append(v)
                    command.append('--input')
                    command.append(uncompressed)
                    command.append('--output')
                    command.append(product)
                    proc = Popen(command)
                    proc.communicate()
                
                # clean up
                self.log.debug('Deleting temp directory %s', query['temp'])
                self.purge_directory(query['temp'])
    
    def varify_directory(self, path):
        result = True
        try:
            if not os.path.exists(path):
                self.log.debug(u'Creating directory %s', path)
                os.makedirs(path)
        except OSError as err:
            self.log.error(unicode(err))
            result = False
        return result
    
    def purge_directory(self, path):
        if os.path.isdir(path):
            for fname in os.listdir(path):
                fpath = os.path.join(path, fname)
                if os.path.isfile(fpath):
                    os.remove(fpath)
            try:
                os.removedirs(path)
            except OSError:
                pass
    
    

def default_json_handler(o):
    result = None
    from bson.objectid import ObjectId
    if isinstance(o, datetime):
        result = o.isoformat()
    if isinstance(o, ObjectId):
        result = str(o)
        
    return result

def decode_cli():
    
    # Global arguments for all commands
    p = ArgumentParser()
    p.add_argument('-v', '--verbosity',             dest='verbosity',   metavar='LEVEL', choices=log_levels.keys(), default='info', help='logging verbosity level [default: %(default)s]')
    p.add_argument('--version',                     action='version', version='%(prog)s 0.1')
    
    # A different parser for every action
    s = p.add_subparsers(dest='action')
    c = {}
    
    c['commit'] = s.add_parser('commit', help='Move new images to buffer directory and add to index')
    
    c['purge'] = s.add_parser('purge',
        help='Delete all images outside the specified window',
        description='TIMESTAMP is given as YYYY-MM-DD HH:MM:SS, DURATION is given as {H}h{M}m{S}s or any subset, i.e. 4h34m'
    )
    c['purge'].add_argument('-t', '--from',        metavar='TIMESTAMP', dest='from timestamp',       help='Start timestamp')
    c['purge'].add_argument('-T', '--to',          metavar='TIMESTAMP', dest='to timestamp',         help='End timestamp')
    c['purge'].add_argument('-L', '--length',      metavar='DURATION', default='180m',   dest='timestamp window', help='Max window size in seconds [default: %(default)s]')
    c['purge'].add_argument('-B', '--backward',    metavar='DURATION', default='0s',   dest='timestamp offset', help='Window offset backward in seconds[default: %(default)s]')
    
    c['pack'] = s.add_parser( 'pack',
        help='Pack jpeg frames to stream.',
        description='TIMESTAMP is given as YYYY-MM-DD HH:MM:SS, DURATION is given as {H}h{M}m{S}s or any subset, i.e. 4h34m'
    )
    c['pack'].add_argument('-p', '--profile', dest='profile', default='default',        help='Video encoder profile [default: %(default)s]')
    c['pack'].add_argument('-t', '--from',        metavar='TIMESTAMP', dest='from timestamp',       help='Start timestamp')
    c['pack'].add_argument('-T', '--to',          metavar='TIMESTAMP', dest='to timestamp',         help='End timestamp')
    c['pack'].add_argument('-L', '--length',      metavar='DURATION', default='180m',   dest='timestamp window', help='Max window size in seconds [default: %(default)s]')
    c['pack'].add_argument('-B', '--backward',    metavar='DURATION', default='0s',   dest='timestamp offset', help='Window offset backward in seconds[default: %(default)s]')
    
    o = {}
    for k,v in vars(p.parse_args()).iteritems():
        if v: o[k] = v
    return o
    

def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    
    env = decode_cli()
    logging.getLogger().setLevel(log_levels[env['verbosity']])
    
    scraper = SurveyScraper(env, config['camera']['nixon'])
    scraper.load()
    
    if env['action'] == 'commit':
        scraper.commit()
        
    if env['action'] == 'purge':
        scraper.purge()
        
    if env['action'] == 'pack':
        scraper.pack()
        
    scraper.unload()


if __name__ == '__main__':
    main()
