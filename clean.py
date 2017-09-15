# remove instructor notes and narration scripts from Python and R scripts
# and delete some files that are not intended for the students

# DANGER: this script is intended to be run within the
# public CDSW project that students will check out
# at the beginning of class. you should not run it in
# any other situation! it deletes contents of files
# without warning then deletes itself.

# regexes matching beginning and end of instructor note
i_n_start_regex = "#[#\s]*BEGIN INSTRUCTOR NOTE"
i_n_stop_regex = "#[#\s]*END INSTRUCTOR NOTE"

# regexes matching beginning and end of narration script
n_s_start_regex = "#[#\s]*BEGIN NARRATION SCRIPT"
n_s_stop_regex = "#[#\s]*END NARRATION SCRIPT"

# prevent this script from running in places where it should not run
cdsw_project = os.environ.get("CDSW_PROJECT")
if cdsw_project is None or cdsw_project.endswith("/edu-data-science-dev"):
  raise Exception("You cannot run this script here!")

for root in ["/home/cdsw/code-python", "/home/cdsw/code-r"]:
	for dir, subdirs, files in os.walk(root):
		for file in files:
			path = os.path.join(dir, file)
			if path.lower().endswith((".r",".py")) and "/." not in path:
				# read file
				with open(path, "r") as f :
					contents = f.read()
				# count how many instructor note starts and stops
				i_n_starts = len(re.findall(i_n_start_regex, contents))
				i_n_stops = len(re.findall(i_n_stop_regex, contents))
				# verify that there are the same number of starts and stops
				if i_n_starts != i_n_stops:
					raise Exception("Mismatched instructor note starts and stops!")
				# count how many narration script starts and stops
				n_s_starts = len(re.findall(n_s_start_regex, contents))
				n_s_stops = len(re.findall(n_s_stop_regex, contents))
				# verify that there are the same number of starts and stops
				if n_s_starts != n_s_stops:
					raise Exception("Mismatched narration script starts and stops!")
				if i_n_starts > 0 or n_s_starts > 0:
					contents = re.sub("\n?" + i_n_start_regex + ".*?" + i_n_stop_regex + "(\n?)", "\\1", contents, flags=re.S)
					# remove narration scripts
					contents = re.sub("\n?" + n_s_start_regex + ".*?" + n_s_stop_regex + "(\n?)", "\\1", contents, flags=re.S)
					# write file
					with open(path, "w") as f:
						f.write(contents)
					print("Cleaned " + path)

# delete files that are not intended for the students
os.remove("/home/cdsw/sections.sh")
          
# this script will self-destruct
os.remove("/home/cdsw/clean.py")
