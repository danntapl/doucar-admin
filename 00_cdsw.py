# # An Introduction to CDSW


# ## Entering Code

# Enter code as you normally would in the shell or a script:

print("Hello, CDSW!")

2 + 2

import random
import matplotlib.pyplot as plt
plt.plot([random.normalvariate(0,1) for i in xrange(1,1000)])


# ## Accessing the Command Line

# Prefix Linux commands with `!`:

!pwd

!ls -l


# ## Working with Python Packages

# **Important:** Packages are managed on a project-by-project basis.

# Use `!pip list` to get a list of the currently installed packages:
!pip list --format=columns

# Search for an available Python package (seaborn in this case):
!pip search seaborn

# Install a new package. Note that this package is now installed for all
# sessions associated with this project.
!pip install seaborn

# Show details about an installed package.  This returns nothing if the package
# is not installed.
!pip show seaborn

# Use seaborn:
import numpy as np
import seaborn as sns
x = np.random.normal(size=100)
sns.distplot(x, rug=True)

# Uninstall package. (Note: Include the '-y' option to avoid a prompt asking
# you to confirm the delete.)
!pip uninstall -y seaborn


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

# Displaying a stored image file:

from IPython.display import Image
Image("spark.png")

# Dummy Python command to get Linux commands to print.  
# See [DSE-2112](https://jira.cloudera.com/browse/DSE-2112).
print("Fin")


# ## References

# [Cloudera Data Science Workbench](https://www.cloudera.com/documentation/data-science-workbench/latest.html)

# [Markdown](https://daringfireball.net/projects/markdown/)

# [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics)
