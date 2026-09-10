# Build-only documentation toolchain. These packages are intentionally not
# declared in pyproject.toml because published DaggerML dependencies stay unchanged.
required <- c(knitr = "1.49", rmarkdown = "2.29", reticulate = "1.40.0", xfun = "0.49")
if (paste(R.version$major, R.version$minor, sep = ".") != "4.4.3") {
  stop("Install R version 4.4.3 for the documentation build")
}
for (package in names(required)) {
  if (!requireNamespace(package, quietly = TRUE) || as.character(packageVersion(package)) != required[[package]]) {
    stop(sprintf("Install %s version %s for the documentation build", package, required[[package]]))
  }
}
