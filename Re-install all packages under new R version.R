#######################################
# Update all packages after R upgrade #
#######################################

#Get currently installed packages
package_df <- as.data.frame(installed.packages("/home/c.stienen/R/x86_64-redhat-linux-gnu-library/3.4/"))
package_list <- as.character(package_df$Package)

#Re-install Install packages
install.packages(package_list)
