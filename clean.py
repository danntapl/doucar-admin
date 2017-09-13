# remove instructor notes and narration scripts from Python and R files

# DANGER: this script is intended to be run within the
# public CDSW project that students will check out
# at the beginning of class. you should not run it in
# any other situation! it deletes contents of files
# without warning then deletes itself.

# currently assumes:

#📝
# This is an instructor note.
#🔚

#🎙
#This is a narration script.
#🔚

# change this later after deciding on the notation to use

import os, re

for root in ["/home/cdsw/code-python", "/home/cdsw/code-r"]:
	for dir, subdirs, files in os.walk(root):
		for file in files:
			path = os.path.join(dir, file)
			if path.lower().endswith((".r",".py")) and "/." not in path:
				# read file
				with open(path, "r") as f :
					contents = f.read()
				if re.search("📝|🎙", contents):
					print("Cleaned " + path)
					# remove instructor notes
					contents = re.sub("\n?#\s*📝.*?🔚(\n?)", "\\1", contents, flags=re.S)
					# remove narration scripts
					contents = re.sub("\n?#\s*🎙.*?🔚(\n?)", "\\1", contents, flags=re.S)
					# write file
					with open(path, "w") as f:
						f.write(contents)

# this script will self-destruct
os.remove("/home/cdsw/clean.py")