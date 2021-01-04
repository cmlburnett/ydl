# System
import hashlib
import html.parser
import xml.etree.ElementTree as ET

# Installed
import requests


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

