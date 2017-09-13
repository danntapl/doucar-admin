# # An Introduction to CDSW

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# ## Entering Code

# Enter code as you normally would in the shell or a script:

print("Hello, CDSW!")

2 + 2

import seaborn as sns
iris = sns.load_dataset("iris")
sns.pairplot(iris, hue="species")


# ## Getting Help

# Use the standard Python help command:
# ``` python
# help(sns.pairplot)
# ```

# or the IPython help syntax:
sns.pairplot?

# Use the following IPython help syntax to inspect the source code:
# ``` python
# sns.pairplot??
# ```

# **Developer Note:** The script does not run correctly (using `Run All`) when
# this command is uncommented.


# ## Accessing the Command Line

# Prefix Linux commands with `!`:

!pwd

!ls -l

# You can also access the command line via the **Terminal access** menu item.


# ## Working with Python Packages

# **Important:** Packages are managed on a project-by-project basis.

# Use `!pip list` to get a list of the currently installed packages:
!pip list --format=columns

# **Note:** Replace `pip` with `pip3` in Python 3.

# Search for the [folium](https://github.com/python-visualization/folium)
# package in the Python package repository:
!pip search folium

# Install a new package:
!pip install folium

# **Note:** This package is now installed for all sessions associated with this
# project.

# Show details about an installed package:
!pip show folium

# **Note:**  This returns nothing if the package is not installed.

# Use package:
import folium
folium.Map(location=[46.8772222, -96.7894444])

# Uninstall package:
!pip uninstall -y folium

# **Note:** Include the `-y` option to avoid an unseen prompt to confirm the uninstall.


# ## Formatting Session Output

# Comments in your file can include
# [Markdown](https://daringfireball.net/projects/markdown/syntax) for
# presentation.  You will see some examples here, and you can see the
# References section below to find out more.

# ### Headings

# # Heading 1

# ## Heading 2

# ### Heading 3

# ### Text

# Plain text

# *Emphasized text* or _emphasized text_

# **Bold text** or __bold text__

# `Code text` (Note these are backtick quotes.)

# ### Mathematical Text

# Display an inline term like $\bar{x} = \frac{1}{n} \sum_{i=1}^{n} x_i$ using
# a [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics) expression
# surrounded by dollar-sign characters.

# A math expression can be displayed set apart by surrounding the LaTeX
# shorthand with double dollar-signs, like so: $$f(x)=\frac{1}{1+e^{-x}}$$

# ### Lists

# Bulleted List
# * Item 1
#   * Item 1a
#   * Item 1b
# * Item 2
# * Item 3

# Numbered List
# 1. Item 1
# 2. Item 2
# 3. Item 3

# ### Links

# Link to [Cloudera](http://www.cloudera.com)

# ### Images

# Display a stored image file:
from IPython.display import Image
Image("resources/spark.png")

# **Note:** The image path is relative to `/home/cdsw/` regardless of script
# location.

# ### Code blocks

# To print a block of code in the output without running it, use a comment line
# with three backticks to begin the block, then the block of code with each
# line preceded with the comment character, then a comment line with three
# backticks to close the block. Optionally include the language name after the
# opening backticks:

# ``` python
# print("Hello, World!")
# ```

# You can omit the language name to print the code block in black text without
# syntax coloring, for example, to display a block of static data or output:

# ```
# Hello, World!
# ```

# ### Invisible comments

#[//]: # (To include a comment that will not appear in the)
#[//]: # (output at all, you can use this curious syntax.)

# Move along, nothing to see here.


# ## Exercises

# (1) Experiment with the CDSW command prompt.  Run some commands and note the
# following:
# * Tab completion is available
# * GNU Readline Library commands such as `C-a`, `C-e`, `C-f`, `C-b`, `C-d`,
# and `C-k` are available
# * The up and down arrows navigate the command history

# (2) Experiment with the CDSW editor.  Create a new file.  Enter Python code
# and Markdown text.  Run the file line by line (or chunk by chunk).  Clear the
# console log and run the entire file.

# (3) Open a terminal window.  Type `env | grep PYTHON` to explore the Python
# environmental variables.


# ## References

# [Cloudera Data Science Workbench](https://www.cloudera.com/documentation/data-science-workbench/latest.html)

# [Markdown](https://daringfireball.net/projects/markdown/)

# [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics)

# [GNU Readline Library](http://cnswww.cns.cwru.edu/php/chet/readline/rluserman.html)
