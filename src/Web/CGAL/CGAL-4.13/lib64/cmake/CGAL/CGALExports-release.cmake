#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "CGAL::CGAL" for configuration "Release"
set_property(TARGET CGAL::CGAL APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(CGAL::CGAL PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib64/libCGAL.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS CGAL::CGAL )
list(APPEND _IMPORT_CHECK_FILES_FOR_CGAL::CGAL "${_IMPORT_PREFIX}/lib64/libCGAL.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
