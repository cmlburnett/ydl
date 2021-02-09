"""
FUSE implementation that allows interactive listing of the videos from the filesystem.
This would allow a video playing system like Plex to access the downloaded videos.
"""

# System
import errno
import glob
import os
import stat
import threading

# Installed
try:
	import fuse
except:
	print("No fuse installed")
	fuse = None

def ydl_fuse(d, root, rootbase, foreground=True, allow_other=False):
	"""
	Invoke fuse.py to create a mount point backed by the YDL database.
	This symlinks each video to the actual data file.
	The point of this is to permit mapping of lists to the data files without manipulating the raw data files.
	For example, playlists could include the index in the playlist and thus allow
	 an ordered viewing of the playlist. If, for example, it was formatted for the playlist to
	 appear as a TV series (eg, "{channel} - s1e{index} - {name}") then an app like Plex could
	 pick up and show the YouTube playlist as a series of episodes like a TV show.

	As this is a virtual overlay over the raw data, no actual manipulation to the data is done and would
	 thus permit representing the exact same data in multiple ways simultaneously.
	"""

	if fuse is None:
		raise Exception("Cannot run fuse, it is not installed")

	fuse.FUSE(_ydl_fuse(d, rootbase), root, nothreads=True, foreground=foreground, allow_other=allow_other)

class fuse_obj:
	_dirperm = stat.S_IFDIR   | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH   | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
	_lnkperm = stat.S_IFLNK   | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

class fuse_obj_root(fuse_obj):
	"""
	Root object for listing directory contents and getting attributes.
	"""

	def __init__(self, db, rootbase):
		self._db = db
		self._dirs = {}
		self._dirlist = ['.','..']
		self._rootbase = rootbase

	def add_dir(self, o):
		self._dirs[o.Path] = o
		self._dirlist.append(o.Path)

	def readdir_len(self, path, index):
		if len(path) == 1 and len(path[0]) == 0:
			# Get the root list of directories
			return len(self._dirlist)
		else:
			if path[0] not in self._dirs:
				raise FuseOSError(errno.ENOENT)

			# Defer to the lists object
			return self._dirs[path[0]].readdir_len(path, index+1)

	def readdir(self, path, index):
		if len(path) == 1 and len(path[0]) == 0:
			# Get the root list of directories
			return self._dirlist
		else:
			if path[0] not in self._dirs:
				raise FuseOSError(errno.ENOENT)

			# Defer to the lists object
			return self._dirs[path[0]].readdir(path, index+1)

	def getattr(self, path, index):
		s = os.lstat(os.path.dirname(self._db.Filename))
		if len(path) == 1 and not len(path[0]):
			# Info for the root directory (the mount point directory)
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._dirperm,
				'st_nlink': 1,
				'st_size': len(self._dirlist) - 2,
			}
		else:
			# Defer to the lists object
			return self._dirs[path[0]].getattr(path, index+1)

class fuse_obj_lists(fuse_obj):
	"""
	First level directory that lists the channels for a particular type (c, ch, u, pl).
	"""

	def __init__(self, db, rootbase, path):
		self._db = db
		self._rootbase = rootbase
		self._table = getattr(db, path)
		self._colname = 'name'
		if path == 'pl':
			self._colname = 'ytid'

		self._path = path

		self._chans = {}

	@property
	def Path(self): return self._path

	def _getchan(self, chan):
		# Get the files object that lists for a specific object
		if chan not in self._chans:
			self._chans[chan] = fuse_obj_files(self._db, self._rootbase, self._table, self._colname)
		return self._chans[chan]

	def getattr(self, path, index):
		s = os.lstat(os.path.dirname(self._db.Filename))
		if len(path) == 1:
			# Get attributes on the channel directory
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._dirperm,
				'st_nlink': 1,
				'st_size': self.readdir_len(path, index),
			}
		elif len(path) >= 2:
			# Defer to files object
			return self._getchan(path[1]).getattr(path, index)

	def readdir_len(self, path, index):
		return len(self.readdir(path, index))


	def readdir(self, path, index):
		ret = ['.', '..']

		if len(path) == 1:
			# Get list of channels in this list
			ret += [_[self._colname] for _ in self._table.select(self._colname)]
		elif len(path) >= 2:
			# Defer to files object
			return self._getchan(path[1]).readdir(path, index)

		return ret

class fuse_obj_files(fuse_obj):
	"""
	Second level directory that lists sym links of a channel to the actual files
	These are created on demand for each channel.
	"""

	def __init__(self, db, rootbase, table, colname):
		self._db = db
		self._rootbase = rootbase
		self._table = table
		self._colname = colname

	def getattr(self, path, index):
		s = os.lstat(os.path.dirname(self._db.Filename))
		if len(path) == 2:
			# Get attributes on the chennel directory
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._dirperm,
				'st_nlink': 1,
				'st_size': self.readdir_len(path, index),
			}
		elif len(path) == 3:
			# Get the info on the video link to the actual data file

			# Determine if relative or absolute path
			if self._rootbase.startswith('..'):
				r = '../../' + self._rootbase
			else:
				r = self._rootbase

			# Form the sym link reference path and get the length of it
			fname = r + "/" + path[1] + '/' + path[2]
			sz = len(fname)

			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._lnkperm,
				'st_nlink': 1,
				'st_size': sz,
			}
		else:
			raise FuseOSError(errno.ENOENT)

	def readdir_len(self, path, index):
		return len(self.readdir(path, index))

	def readdir(self, path, index):
		ret = ['.','..']

		if len(path) == 2:
			# Get files
			res = self._db.v.select(['ytid','name'], '`dname`=? and `utime` is not null', [path[1]])
			rows = [dict(_) for _ in res]

			for r in rows:
				alias = self._db.vnames.select_one('name', '`ytid`=?', [r['ytid']])
				if alias:
					r['name'] = alias['name']
				ret.append("%s-%s.mkv" % (r['name'], r['ytid']))

		return ret

	def path(self, ytid):
		# Make the path for the given ytid

		if self._rootbase.startswith('..'):
			r = '../../' + self._rootbase
		else:
			r = self._rootbase

		row = self._db.v.select_one(['ytid', 'name'], '`ytid`=?', [ytid])

		alias = self._db.vnames.select_one(['name'], '`ytid`=?', [ytid])
		if alias:
			row['name'] = alias['name']

		return r + "/" + row['name'] + '-' + row['ytid'] + '.mkv'

class fuse_obj_videos(fuse_obj):
	"""
	First level directory tat lists the videos in different ways.
	"""

	def __init__(self, db, rootbase, path):
		self._db = db
		self._rootbase = rootbase
		self._table = db.v
		self._path = path
		self._dirlist = []
		self._dirs = {}

		self.add_dir( fuse_obj_videos_by_date(db, rootbase, 'date_publish', 'ptime') )
		self.add_dir( fuse_obj_videos_by_date(db, rootbase, 'date_download', 'utime') )

	def add_dir(self, o):
		self._dirs[o.Path] = o
		self._dirlist.append(o.Path)

	@property
	def Path(self): return self._path

	def getattr(self, path, index):
		s = os.lstat(os.path.dirname(self._db.Filename))
		if len(path) == 1:
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._dirperm,
				'st_nlink': 1,
				'st_size': len(self._dirlist),
			}
		else:
			return self._dirs[ path[1] ].getattr(path, index+1)

	def readdir_len(self, path, index):
		return len(self.readdir(path, index))

	def readdir(self, path, index):
		if len(path) == 1:
			return ['.','..'] + self._dirlist
		elif len(path) >= 2:
			return self._dirs[ path[1] ].readdir(path, index+1)
		else:
			raise FuseOSError(errno.ENOENT)

class fuse_obj_videos_by_date(fuse_obj):
	def __init__(self, db, rootbase, path, colname):
		self._db = db
		self._rootbase = rootbase
		self._table = db.v
		self._colname = colname
		self._path = path

	@property
	def Path(self): return self._path

	def getattr(self, path, index):
		s = os.lstat(os.path.dirname(self._db.Filename))

		# directory, year, month, and day
		if len(path) in (2,3,4,5):
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._dirperm,
				'st_nlink': 1,
				'st_size': self.readdir_len(path, index),
			}
		elif len(path) == 6:
			# Get the info on the video link to the actual data file

			fname = _ydl_fuse.video_path(self._db, self._rootbase, path)
			sz = len(fname)

			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': self._lnkperm,
				'st_nlink': 1,
				'st_size': sz,
			}


	def readdir_len(self, path, index):
		return len(self.readdir(path, index))

	def readdir(self, path, index):
		ret = ['.','..']

		# List years
		if len(path) == 2:
			res = self._db.execute("select strftime('%%Y', `%s`) as year from v where utime is not null group by year" % self._colname)
			ret += [_['year'] for _ in res]

		# List months
		elif len(path) == 3:
			res = self._db.execute("select strftime('%%m', `%s`) as month from v where strftime('%%Y', `ptime`)=? and `utime` is not null group by month" % self._colname, (path[2],))
			ret += [_['month'] for _ in res]

		# List days
		elif len(path) == 4:
			res = self._db.execute("select strftime('%%d', `%s`) as day from v where strftime('%%Y-%%m', `ptime`)=? and `utime` is not null group by day" % self._colname, (path[2] + '-' + path[3],))
			ret += [_['day'] for _ in res]

		# List videos
		elif len(path) == 5:
			res = self._db.v.select(['dname', 'name','ytid'], 'strftime("%%Y-%%m-%%d", `%s`)=? and `utime` is not null' % self._colname, ['-'.join(path[2:5])])
			ret += [_['dname'] + '-' + _['name'] + '-' + _['ytid'] + '.mkv' for _ in res]

		return ret


class _ydl_fuse(fuse.LoggingMixIn, fuse.Operations):
	"""
	Class that implements the FUSE file system functionality.
	"""

	def __init__(self, d, rootbase):
		"""
		@d is the instance of db class in __main__ that accesses ydl.db.
		@rootbase is the root base to prepend to all sym links that goes from the
		 displayed file to the actual datafile. It is calculated externally from
		 the location of ydl.db and the mount point and passed into this class.
		"""

		self._db = d
		self._root = os.path.dirname(d.Filename)
		self._rootbase = rootbase
		self._lock = threading.Lock()

		# If set to True, directory listings will be slower
		self._set_accurate_stat_times = False

		# Format of file names of videos
		#self._fileformat = '{ctime}-{name}-{ytid}.mkv'
		self._fileformat = '{name}-{ytid}.mkv'
		# WARNING: readlink() currently assumes the path ends with YTID.SUFFIX
		# if you use the slow method and that the YTID is 11 characters long
		# So if you change the format and break this expectation, the links won't work

		self._fs = fuse_obj_root(self._db, rootbase)
		self._fs.add_dir(fuse_obj_lists(self._db, rootbase, 'c'))
		self._fs.add_dir(fuse_obj_lists(self._db, rootbase, 'ch'))
		self._fs.add_dir(fuse_obj_lists(self._db, rootbase, 'u'))
		self._fs.add_dir(fuse_obj_lists(self._db, rootbase, 'pl'))
		self._fs.add_dir(fuse_obj_videos(self._db, rootbase, 'v'))

	# Root directory of the YDL library in which the database and video files reside
	@property
	def root(self): return self._root

	# Lock probably isn't needed since it's read only?
	@property
	def lock(self): return self._lock

	def access(self, path, mode):
		# Read only
		if mode | os.W_OK:
			return False
		else:
			return True

	# Can't change mode or owner
	def chmod(self, path, mode):
		raise fuse.FuseOSError(errno.EACCES)
	def chown(self, path, mode):
		raise fuse.FuseOSError(errno.EACCES)

	def getattr(self, path, fh):
		"""Get stat() attributes"""

		parts = path[1:].split('/')
		return self._fs.getattr(parts, 0)

	def readdir(self, path, fh):
		"""
		Read the contents of the directory in @path.
		If of the root, it lists the various channel types.
		If of a channel type, then all the names of the channels of that time.
		If of a specific channel, then all the videos currently downloaded from that channel.
		"""

		parts = path[1:].split('/')
		return self._fs.readdir(parts, 0)

	def readlink(self, path):
		"""
		Read the contents of the video symlink that returns the path
		 to the actual data file.
		"""

		path = path.split('/')[1:]

		return type(self).video_path(self._db, self._rootbase, path)

	@classmethod
	def video_path(cls, db, rootbase, path):
		# Shortcut if True
		if True:
			if rootbase.startswith('..'):
				r = '../../' + rootbase
			else:
				r = rootbase

			if path[1] in ('c', 'ch', 'u', 'pl'):
				# 'foo'
				chan = path[-2]
				# 'bar-YTID.mkv'
				fname = path[-1]

				# This is a fixed format
				return r + '/' + chan + '/' + fname

			elif path[0] == 'v':
				# '/v/2020/11/28/NAME-YTID.mkv' -> ['v', '2020', '11', '28', 'NAME-YTID.mkv']

				fname = path[-1]
				fnameroot = os.path.splitext(fname)[0]
				subparts = fnameroot.split('-', 1)

				return r + '/' + subparts[0] + '/' + subparts[1] + '.mkv'
			else:
				raise FuseOSError(errno.ENOENT)


		# Full parsing, if needed then set to False above
		else:
			# str to path list as supplied
			# '/c/foo/bar-YTID.mkv' -> ['c', 'foo', 'bar-YTID.mkv']
			# 'foo'
			chan = path[-2]
			# 'bar-YTID.mkv'
			fname = path[-1]
			# 'bar-YTID'
			fnameroot = os.path.splitext(fname)[0]
			# 'YTID'
			ytid = fnameroot[-11:]

			# Get the name as it's not necessarily te rest of the fname value
			r = db.v.select_one('name', '`ytid`=?', [ytid])
			name = r['name']

			alias = db.vnames.select_one('name', '`ytid`=?', [ytid])
			if alias:
				name = alias['name']

			if rootbase.startswith('..'):
				r = '../../' + rootbase
			else:
				r = rootbase

			# This is a fixed format in the actual data files of name-ytid.mkv
			return r + '/{channel}/{name}-{ytid}.mkv'.format(channel=chan, name=name, ytid=ytid)

	# Cannot do any of this
	def mknod(self, path, mode, dev):
		raise fuse.FuseOSError(errno.EACCES)
	def rmdir(self, path):
		raise fuse.FuseOSError(errno.EACCES)

	# Consider permitting this function that then adds that particular channel to the database
	def mkdir(self, path, mode):
		raise fuse.FuseOSError(errno.EACCES)

	def statfs(self, path):
		# These numbers don't really have any meaning since there's no writing
		# and it exists purely virtual
		return {
			'f_bavail': 0,
			'f_bfree': 0,
			'f_blocks': 1024,
			'f_bsize': 4096,
			'f_favail': 0,
			'f_ffree': 0,
			'f_files': 1024,
			'f_flag': os.ST_RDONLY,
			'f_frsize': 1024,
			'f_namemax': 256,
		}

	# Can't do any of this
	def unlink(self, path):
		raise fuse.FuseOSError(errno.EACCES)

	def symlink(self, name, target):
		raise fuse.FuseOSError(errno.EACCES)

	def rename(self, old, new):
		raise fuse.FuseOSError(errno.EACCES)

	def link(self, target, name):
		raise fuse.FuseOSError(errno.EACCES)

	# TODO: consider permitting this function to trigger a --sync-list and/or --sync-videos and/or --download
	# No facility currently exists to pass this kind of message anywhere as ydl isn't ran as a daemon
	def utimens(self, path, times=None):
		raise fuse.FuseOSError(errno.EACCES)

	# No file operations are permitted as its all sym links elsewhere
	# TODO: permit special files that allow editing of things in the database
	def open(self, path, flags):
		raise fuse.FuseOSError(errno.EACCES)

	def create(self, path, mode, fi=None):
		raise fuse.FuseOSError(errno.EACCES)

	def read(self, path, length, offset, fh):
		raise fuse.FuseOSError(errno.EACCES)

	def write(self, path, buf, offset, fh):
		raise fuse.FuseOSError(errno.EACCES)

	def truncate(self, path, length, fh=None):
		raise fuse.FuseOSError(errno.EACCES)

	def flush(self, path, fh):
		raise fuse.FuseOSError(errno.EACCES)

	def fsync(self, path, fdatasync, fh):
		raise fuse.FuseOSError(errno.EACCES)
