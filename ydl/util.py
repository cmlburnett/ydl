# System
import errno
import hashlib
import html.parser
import os
import stat
import threading
import xml.etree.ElementTree as ET

# Installed
import requests
import fuse


def sec_str(sec):
	"""
	Convert integer seconds to HHH:MM:SS formatted string
	Returns as HHH:MM:SS, MM:SS, or 0:SS with zero padding except for the most significant position.
	"""

	min,sec = divmod(sec, 60)
	hr,min = divmod(min, 60)

	if hr > 0:
		return "%d:%02d:%02d" % (hr,min,sec)
	elif min > 0:
		return "%d:%02d" % (min,sec)
	else:
		return "0:%d" % sec

def inputopts(txt):
	"""
	Pose an input prompt and parse the options.
	The options are defined as letters continaed in parentheses.
	If a capital letter is provided, then that is the default if no option is provided;
	 otherwise an option must be explicitly provided.

	For example, "create directory: (Y)es or (n)? "
	- If user puts in Y or y, it will return y.
	- If user puts in N or n, it will return n.
	- If user puts in nothing and just hits enter, it will return y.

	Should the user provide an unrecognized input, it will loop back infinitely until they do.
	"""

	# Search for all input options
	opts = re.findall("\([a-zA-Z0-9]+\)", txt)
	opts = [_[1:-1] for _ in opts]

	# Find the first one that is all upper case
	default = [_ for _ in opts if _.isupper()]
	if len(default):
		default = default[0]
	else:
		default = None

	# Convert all options to lower case
	opts = [_.lower() for _ in opts]

	# Loop infinitely until a valid input is given
	while True:
		# Query the user
		ret = input(txt)

		# Empty string means they just hit enter, look for a default option
		if not len(ret):
			if default:
				return default
			else:
				continue
		# If something provideed is in the list then accept the lower case version of it
		elif ret.lower() in opts:
			return ret.lower()
		# Repeat
		else:
			print("Option '%s' not recognized, try again" % ret)
			continue

class RSSHelper:
	"""
	Simple helper class for dealing with URLs and RSS URL's.
	Sub class RSSParse is an HTML parser that looks for the link tag for an RSS URL.

	Function GetByPage() is given an HTML page URL and returns the found RSS URL in the page, or False if not found.
	Function ParseRSS_YouTube() assumes RSS URL given is to YouTube and returns the entires it finds.
	"""

	class RSSParse(html.parser.HTMLParser):
		"""
		Parse an HTML page for it's RSS URL.
		End parsing by throwing a GotRSSUrl excpetion when found.
		"""
		def handle_starttag(self, tag, attrs):
			if tag == 'link':
				attrs = dict(attrs)
				if 'type' in attrs and attrs['type'] == 'application/rss+xml':
					raise RSSHelper.GotRSSUrl(attrs['href'])

	class GotRSSUrl(Exception):
		"""
		Exception to return the RSS url once found when parsing HTML.
		"""
		pass

	@classmethod
	def GetByPage(cls, url):
		"""
		Get RSS from page url @url.
		"""

		r = requests.get(url)
		if r.status_code != 200:
			return False

		# Get HTML
		html = r.text

		try:
			RSSHelper.RSSParse().feed(html)

			# Not found as parsing completed
		except RSSHelper.GotRSSUrl as r:
			# Got RSS url (expected outcome is to throw exception and not finish parsing)
			return str(r)
		except:
			# Some other error (maybe parsing error)
			return False

		return False

	@classmethod
	def ParseRSS_YouTube(cls, url):
		"""
		Parse RSS feed at a YouTube url @url and return the available videos from that feed.
		"""

		r = requests.get(url)
		if r.status_code != 200:
			return False

		ret = {
			'title': None,
			'uploader': None,
			'ytids': []
		}

		# Parse RSS as XML
		root = ET.fromstring(r.text)

		title = root.find('./{http://www.w3.org/2005/Atom}title')
		if title is not None:
			ret['title'] = title.text

		uploader = root.find('./{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name')
		if uploader is not None:
			ret['uploader'] = uploader.text

		entries = root.findall('./{http://www.w3.org/2005/Atom}entry')
		for entry in entries:
			ytid = entry.find('./{http://www.youtube.com/xml/schemas/2015}videoId').text
			ret['ytids'].append(ytid)

		return ret

def list_to_quoted_csv(l):
	"""
	Convert a list to a quoted csv string

	['abcd','efgh'] -> "'abcd','efgh'"
	"""

	return ",".join(["'%s'" % _ for _ in l])

def bytes_to_str(v, base2=True):
	if base2:
		k = v / (1024**1)
		m = v / (1024**2)
		g = v / (1024**3)
		t = v / (1024**4)

		if t > 1: return "%.3f TiB" % t
		elif g > 1: return "%.3f GiB" % g
		elif m > 1: return "%.3f MiB" % m
		elif k > 1: return "%.3f KiB" % k
		else:
			return "%d B" % v

	else:
		k = v / (1000**1)
		m = v / (1000**2)
		g = v / (1000**3)
		t = v / (1000**4)

		if t > 0: return "%.3f TB" % t
		elif g > 0: return "%.3f GB" % g
		elif m > 0: return "%.3f MB" % m
		elif k > 0: return "%.3f KB" % k
		else:
			return "%d B" % v

def ytid_hash(v, r):
	"""
	Take the SHA256 hash of the YTID @v, use hash as an integer, then modulus against @r.
	This should equally distribute a sufficiently large collection of YTID's across @r buckets.
	And for the same (YTID, r) pair, the value should be identical for forever.
	"""

	if type(v) is not str:
		raise TypeError("Expected first argument to be a string, got %s" % type(v))
	if type(r) is not int:
		raise TypeError("Expected second argument to be an int, got %s" % type(r))
	if r < 1:
		raise ValueError("Expected modulus to be positive number, got %s" % r)

	m = hashlib.sha256()
	# Can only hash binary values, so make it ASCII
	m.update(v.encode('ascii'))
	# Gets a string of hex characters
	h = m.hexdigest()

	# Convert to an integer (base 16) then modulus
	return int(h,16) % r

def ytid_hash_remap(v, r_old, r_new):
	"""
	Remapping YTID from @r_old to @r_new.
	Use this to determine of the YTID @v is changing buckets with change in modulus.
	This is useful if the number of buckets is determined by number of total items,
	 this will permit easier determination if the files need to move locations.

	Returned is a tuple of (old modulus, new modulus, boolean indicating if different).
	The third item saves the inevitable comparison in dermining if to move or not:
	 if True, then bucket has changed; if False, then bucket is identical.

	For example, if the hash were 20 and # of buckets were changing from 4 to 5, the bucket
	 is zero each time, so ret[2] is False. No bucket change.
	Non-trivial modulus changes *can* result in non-movement of items in buckets.

	In short, calling this function should be faster as the hash is computed only once.
	"""

	if type(v) is not str:
		raise TypeError("Expected first argument to be a string, got %s" % type(v))
	if type(r_old) is not int:
		raise TypeError("Expected second argument to be an int, got %s" % type(r_old))
	if type(r_new) is not int:
		raise TypeError("Expected third argument to be an int, got %s" % type(r_new))
	if r_old < 1:
		raise ValueError("Expected modulus to be positive number for second argument, got %s" % r_old)
	if r_new < 1:
		raise ValueError("Expected modulus to be positive number for third argument, got %s" % r_new)

	m = hashlib.sha256()
	m.update(v.encode('ascii'))
	h = m.hexdigest()

	x = int(h,16)

	z = (x % r_old, x % r_new)

	return (z[0], z[1], z[0] == z[1])

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

	fuse.FUSE(_ydl_fuse(d, rootbase), root, nothreads=True, foreground=foreground, allow_other=allow_other)

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
		dirperm = stat.S_IFDIR   | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH   | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
		lnkperm = stat.S_IFLNK   | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH

		s = os.lstat(os.path.dirname(self._db.Filename))
		if path == '/':
			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': dirperm,
				'st_nlink': s.st_nlink,
				'st_size': s.st_size,
			}
		elif path in ('/c', '/ch', '/u', '/pl'):
			if path == '/c':
				sz = self._db.c.num_rows()
			elif path == '/ch':
				sz = self._db.ch.num_rows()
			elif path == '/u':
				sz = self._db.u.num_rows()
			elif path == '/pl':
				sz = self._db.pl.num_rows()
			else:
				raise NotImplementedError

			return {
				'st_atime': s.st_atime,
				'st_ctime': s.st_ctime,
				'st_mtime': s.st_mtime,

				'st_gid': s.st_gid,
				'st_uid': s.st_uid,
				'st_mode': dirperm,
				'st_nlink': 1,
				'st_size': sz,
			}

		# List channel directories
		elif path.startswith('/c/') or path.startswith('/ch/') or path.startswith('/u/') or path.startswith('/pl/'):
			parts = path.split('/')

			if len(parts) == 3:
				sz = self._db.v.num_rows('`dname`=? and `utime` is not null', [parts[-1]])

				return {
					'st_atime': s.st_atime,
					'st_ctime': s.st_ctime,
					'st_mtime': s.st_mtime,

					'st_gid': s.st_gid,
					'st_uid': s.st_uid,
					'st_mode': dirperm,
					'st_nlink': 1,
					'st_size': sz,
				}
			elif len(parts) == 4:
				chan = parts[-2]
				fname = parts[-1]
				fnameroot = os.path.splitext(fname)[0]

				ytid = fnameroot[-11:]

				# Assume basic times from the database file itself
				atime = s.st_atime
				ctime = s.st_ctime
				mtime = s.st_mtime

				# If desired to show accurate times, this will slow down directory
				# listings significantly as one directory listing will require
				# a separate query per file
				# (or cache these things)
				if self._set_accurate_stat_times:
					# If video has data, then use that instead
					row = self._db.v.select_one(['ptime','ctime','atime','utime'], '`ytid`=?', [ytid])
					if row:
						ctime = row['ptime'].timestamp()
						atime = row['utime'].timestamp()
						mtime = row['atime'].timestamp()


				if self._rootbase.startswith('..'):
					r = '../../' + self._rootbase
				else:
					r = self._rootbase

				# This is a fixed format
				p = r + '/{channel}/{fname}'.format(channel=chan, fname=fname)

				return {
					'st_atime': atime,
					'st_ctime': ctime,
					'st_mtime': mtime,

					'st_gid': s.st_gid,
					'st_uid': s.st_uid,
					'st_mode': lnkperm,
					'st_nlink': 1,
					'st_size': len(p),
				}

			else:
				raise fuse.FuseOSError(errno.EACCES)

		else:
			raise fuse.FuseOSError(errno.EACCES)


	def readdir(self, path, fh):
		"""
		Read the contents of the directory in @path.
		If of the root, it lists the various channel types.
		If of a channel type, then all the names of the channels of that time.
		If of a specific channel, then all the videos currently downloaded from that channel.
		"""

		if path == '/':
			return ['.', '..', 'c', 'ch', 'u', 'pl']

		# List names of each type of list
		elif path == '/c':
			ret = ['.', '..']

			ret += [_['name'] for _ in self._db.c.select('name')]

			return ret
		elif path == '/ch':
			ret = ['.', '..']

			ret += [_['alias'] or _['name'] for _ in self._db.ch.select(['name','alias'])]

			return ret
		elif path == '/u':
			ret = ['.', '..']

			ret += [_['name'] for _ in self._db.u.select('name')]

			return ret
		elif path == '/pl':
			ret = ['.', '..']

			ret += [_['ytid'] for _ in self._db.pl.select('ytid')]

			return ret

		# Read video contents of each list
		elif path.startswith('/c/') or path.startswith('/ch/') or path.startswith('/u/') or path.startswith('/pl/'):
			ret = ['.', '..']

			parts = path.split('/')

			res = self._db.v.select(['ytid','name','ptime'], '`dname`=? and `utime` is not null', [parts[2]])
			ytids = {_['ytid']:dict(_) for _ in res}
			for d in ytids.values():
				d['ctime'] = d['ptime'].strftime('%Y-%m-%d')
				del d['ptime']

			res = self._db.vnames.select(['name','ytid'], '`ytid` in (%s)' % list_to_quoted_csv(ytids.keys()))
			for _ in res:
				# Update the name
				ytids[ _['ytid'] ]['name'] = _['name']

			# Make file names
			for d in ytids.values():
				_ = self._fileformat.format(**d)
				ret.append(_)

			return ret

		else:
			return ['.', '..']

	def readlink(self, path):
		"""
		Read the contents of the video symlink that returns the path
		 to the actual data file.
		"""

		# Shortcut if True
		if True:
			# '/c/foo/bar-YTID.mkv' -> ['', 'c', 'foo', 'bar-YTID.mkv']
			parts = path.split('/')
			# 'foo'
			chan = parts[-2]
			# 'bar-YTID.mkv'
			fname = parts[-1]

			if self._rootbase.startswith('..'):
				r = '../../' + self._rootbase
			else:
				r = self._rootbase

			# This is a fixed format
			return r + '/' + chan + '/' + fname

		# Full parsing, if needed then set to False above
		else:
			# '/c/foo/bar-YTID.mkv' -> ['', 'c', 'foo', 'bar-YTID.mkv']
			parts = path.split('/')
			# 'foo'
			chan = parts[-2]
			# 'bar-YTID.mkv'
			fname = parts[-1]
			# 'bar-YTID'
			fnameroot = os.path.splitext(fname)[0]
			# 'YTID'
			ytid = fnameroot[-11:]

			# Get the name as it's not necessarily te rest of the fname value
			r = self._db.v.select_one('name', '`ytid`=?', [ytid])
			name = r['name']

			alias = self._db.vnames.select_one('name', '`ytid`=?', [ytid])
			if alias:
				name = alias['name']

			if self._rootbase.startswith('..'):
				r = '../../' + self._rootbase
			else:
				r = self._rootbase

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

