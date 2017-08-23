# # An Introduction to CDSW


# ## Entering Code

# Enter code as you normally would in the shell or a script:

print("Hello, CDSW!")

2 + 2

library(ggplot2)
ggplot(data = iris, aes(x = Sepal.Length, y = Sepal.Width, color = Species)) + 
	geom_point()

# ## Getting Help

help("geom_point")

?geom_point

# ## Accessing the Command Line

# You can enclose Linux commands in `system("")`:

system("pwd")

system("ls -l")

# But it's more common to use built-in R functions for tasks like these

getwd()

file.info(list.files())


# ## Working with R Packages

# **Important:** Packages are managed on a project-by-project basis.

# Show what packages are loaded in this session of R
search()
sessionInfo()

# Show the currently installed packages
rownames(installed.packages())

# See if a particular package is installed
"leaflet" %in% rownames(installed.packages())

# Install the current released version of a package from CRAN
install.packages("leaflet")

# Or install the latest development version from GitHub
devtools::install_github("rstudio/leaflet")

# Note that this package is now installed for all sessions associated with this project.

# See what version of the package is installed
packageVersion("leaflet")

# Load the package
library(leaflet)

# Use the package
leaflet() %>%
  addTiles() %>%
  addMarkers(lng = -122.139869, lat = 37.425501, popup = "395 Page Mill Rd")

# Unload the package
detach("package:leaflet", unload = TRUE)

# Uninstall the package
#``` r
#remove.packages("leaflet")
#```
