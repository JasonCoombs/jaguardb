#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "CGAL::CGAL_ImageIO" for configuration "Release"
set_property(TARGET CGAL::CGAL_ImageIO APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(CGAL::CGAL_ImageIO PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib64/libCGAL_ImageIO.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS CGAL::CGAL_ImageIO )
list(APPEND _IMPORT_CHECK_FILES_FOR_CGAL::CGAL_ImageIO "${_IMPORT_PREFIX}/lib64/libCGAL_ImageIO.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
