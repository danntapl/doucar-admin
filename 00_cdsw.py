# # An Introduction to CDSW


# ## Entering Code

# Enter code as you normally would in a source file:

print("Hello, CDSW!")

2 + 2


# ## Accessing the Command Line

# Prefix Linux commands with `!`:

!pwd

!ls -l


# ## Formatting Session Output

# Comments in your file can include Markdown for presentation.  You'll see some 
# examples here, and you can see the References section below to find out more.

# ### Headings

# # Heading 1

# ## Heading 2

# ### Heading 3

# ### Text Formats

# Plain text

# *Emphasized text* or _emphasized text_

# **Bold text** or __bold text__

# `Code text` (Note these are backtick quotes.)

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


# ### Math formulas

# Display an inline term like $\displaystyle\sum_{i=1}^{N} t_i$ using
# a TeX expression surrounded by dollar-sign characters.


# A math expression can be displayed set apart by surrounding the TeX 
# shorthand with double dollar-signs,
# like so: $$f(x)=\frac{1}{1+e^{-x}}$$


# ### Displaying images

# Displaying a stored image file:

from IPython.display import Image

Image("spark.png")

# Creating a simple plot:

import matplotlib.pyplot as plt
import random
plt.plot([random.normalvariate(0,1) for i in xrange(1,1000)])


# ## Working with Python Packages

# **Important:** Packages are managed on a project-by-project basis.

# Use `!pip list` to get a list of the currently installed packages:
!pip list --format=columns

# Search for an available Python package (avro in this case).
!pip search avro

# Install a new package. Note that this is now installed for all sessions in 
# this project.
!pip install avro

# Show details about an installed package.  This returns nothing if the package is not installed.
!pip show avro

# Uninstall package. (Note: Include the '-y' option to avoid a prompt asking you to confirm 
# the delete.)
!pip uninstall -y avro

# Dummy Python command to get Linux commands to print.  
# See [DSE-2112](https://jira.cloudera.com/browse/DSE-2112).
print("Fin")


# ## References

# Workbench: [Cloudera Data Science Workbench Documentation](https://www.cloudera.com/documentation/data-science-workbench/latest.html)

# Markdown syntax: [Daring Fireball Markdown](https://daringfireball.net/projects/markdown/)

# LaTeX for mathematical expressions: [LaTeX/Mathematics](https://en.wikibooks.org/wiki/LaTeX/Mathematics)

