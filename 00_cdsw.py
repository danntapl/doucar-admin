# # An Introduction to CDSW


# ## Entering Code

# Enter code as you normally would in a source file:

print "Hello, CDSW!"

2 + 2


# ## Accessing the Command Line

# Prefix Linux commands with `!`:

!pwd

!ls -l

!hdfs dfs -ls


# ## Working with Python Packages

# **Important:** Packages are managed on a project-by-project basis.

# Use `!pip list` to get a list of the currently installed packages:
!pip list


# ## Formatting the Session Output

# ### Headings

# # Heading 1
# ## Heading 2
# ### Heading 3

# ### Text Formats

# Plain text

# *Emphasized text* or _emphasized text_

# **Bold text** or __bold text__

# `Code text`

# ### Lists

# Bulleted List
# * Item 1
#   * Item 1a
#   * Item 2b
# * Item 2
# * Item 3

# Numbered List
# 1. Item 1
# 2. Item 2
# 3. Item 3

# ### Links

# Link to [Cloudera](http://www.cloudera.com)


# ## References

# [Cloudera Data Science Workbench Documentation](https://www.cloudera.com/documentation/data-science-workbench/latest.html)

# [Daring Fireball Markdown](https://daringfireball.net/projects/markdown/)


# ## TODO

# * Installing and removing Python packages
# * Add additional formatting options


# Dummy Python command to get Linux commands to print.  See [DSE-2112](https://jira.cloudera.com/browse/DSE-2112).
print('Fin')