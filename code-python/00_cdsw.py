# # An Introduction to CDSW


# ## Entering Code

# Enter code as you normally would in the shell or a script:

print("Hello, CDSW!")

2 + 2

import seaborn as sns
iris = sns.load_dataset("iris")
sns.pairplot(iris, hue="species")


# ## Getting Help

# Use the standard Python help command:
# help(sns.pairplot)

# or the IPython help syntax:
sns.pairplot?

# Use the following IPython help syntax to inspect the source code:
#sns.pairplot??

# **Developer Note:** The script does not run (using `Run All`) correctly when
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

# Search for an available Python package (folium in this case):
!pip search folium

# Install a new package. Note that this package is now installed for all
# sessions associated with this project.
!pip install folium

# Show details about an installed package.  This returns nothing if the package
# is not installed.
!pip show folium

# Use folium:
import folium
folium.Map(location=[46.8772222, -96.7894444])

# Uninstall package. (Note: Include the '-y' option to avoid a prompt asking
# you to confirm the delete.)
!pip uninstall -y folium


# ## Formatting Session Output

# Comments in your file can include
# [Markdown](https://daringfireball.net/projects/markdown/syntax) for
# presentation.  You'll see some examples here, and you can see the References
# section below to find out more.

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

# **Note:** The image path is relative to `/home/cdsw/` regardless of script location.


# ## Exercises

# (1) Experiment with the CDSW command prompt.  Note the following:
# * Tab completion is available
# * Some GNU Readline Library commands such as `C-f`, `C-b`, and `C-d` are
# available.

# (2) Experiment with the CDSW editor.  Create a new file.  Enter Python code
# and Markdown text.  Run the file line by line (or chunk by chunk).  Clear the
# console log and run the entire file.

# (3) Open a terminal window.  Type `env | grep PYTHON` to explore the Python
# environmental variables


# ## References

# [Cloudera Data Science Workbench](https://www.cloudera.com/documentation/data-science-workbench/latest.html)

# [Markdown](https://daringfireball.net/projects/markdown/)

# [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics)
