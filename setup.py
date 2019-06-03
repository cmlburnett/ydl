from distutils.core import setup

majv = 1
minv = 0

setup(
	name = 'ydl',
	version = "%d.%d" %(majv,minv),
	description = "Python module that wraps youtube-dl",
	author = "Colin ML Burnett",
	author_email = "cmlburnett@gmail.com",
	url = "",
	packages = ['ydl'],
	package_data = {'ydl': ['ydl/__init__.py']},
	classifiers = [
		'Programming Language :: Python :: 3.4'
	]
)
