# System
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

