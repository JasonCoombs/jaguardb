#
# This files contains definitions needed to use CGAL in a program.
# DO NOT EDIT THIS. The definitons have been generated by CMake at configuration time.
# This file is loaded by cmake via the command "find_package(CGAL)"
#
# This file correspond to a CGAL installation with "make install", thus the actual location
# must be given by the cmake variable or enviroment variable CGAL_DIR.

set(CGAL_CONFIG_LOADED TRUE)

get_filename_component(CGAL_CONFIG_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)

set(CGAL_HEADER_ONLY "OFF" )

# CGAL_DIR is the directory where this CGALConfig.cmake is installed
string(REPLACE "lib64/cmake/CGAL" "" CGAL_INSTALL_PREFIX "${CGAL_CONFIG_DIR}")

 if(NOT EXISTS "${CGAL_INSTALL_PREFIX}/lib64/cmake/CGAL/CGALConfig.cmake")
    # Cannot compute CGAL_INSTALL_PREFIX!
    # Use the CMake prefix chosen at compile time.
    set(CGAL_INSTALL_PREFIX "/home/dev2/src/Web/CGAL/CGAL-4.13")
 endif()

set(CGAL_MAJOR_VERSION    "4" )
set(CGAL_MINOR_VERSION    "13" )
set(CGAL_BUGFIX_VERSION   "0" )
set(CGAL_BUILD_VERSION    "900" )
set(CGAL_SCM_BRANCH_NAME  "n/a")
set(CGAL_GIT_SHA1         "")

set(CGAL_BUILD_SHARED_LIBS        "FALSE" )
set(CGAL_Boost_USE_STATIC_LIBS    "OFF" )

set(CGAL_CXX_FLAGS_INIT                   "" )
set(CGAL_CXX_FLAGS_RELEASE_INIT           "-O3 -DNDEBUG" )
set(CGAL_CXX_FLAGS_DEBUG_INIT             "-g" )
set(CGAL_MODULE_LINKER_FLAGS_INIT         "" )
set(CGAL_MODULE_LINKER_FLAGS_RELEASE_INIT "" )
set(CGAL_MODULE_LINKER_FLAGS_DEBUG_INIT   "" )
set(CGAL_SHARED_LINKER_FLAGS_INIT         "" )
set(CGAL_SHARED_LINKER_FLAGS_RELEASE_INIT "" )
set(CGAL_SHARED_LINKER_FLAGS_DEBUG_INIT   "" )
set(CGAL_BUILD_TYPE_INIT                  "Release" )

set(CGAL_INCLUDE_DIRS  "${CGAL_INSTALL_PREFIX}/include" )
set(CGAL_MODULES_DIR   "${CGAL_INSTALL_PREFIX}/lib64/cmake/CGAL" )
set(CGAL_LIBRARIES_DIR "${CGAL_INSTALL_PREFIX}/lib64" )

# If CGAL_ImageIO is built, tell if it was linked with Zlib.
set(CGAL_ImageIO_USE_ZLIB                 "ON" )

set(CGAL_VERSION "${CGAL_MAJOR_VERSION}.${CGAL_MINOR_VERSION}.${CGAL_BUGFIX_VERSION}")

set(CGAL_USE_FILE "${CGAL_MODULES_DIR}/UseCGAL.cmake" )
set(CGAL_GRAPHICSVIEW_PACKAGE_DIR "${CGAL_INCLUDE_DIRS}/CGAL/" CACHE INTERNAL "Directory containing the GraphicsView package")

if ( CGAL_FIND_REQUIRED )
  set( CHECK_CGAL_COMPONENT_MSG_ON_ERROR TRUE        )
  set( CHECK_CGAL_COMPONENT_ERROR_TYPE   FATAL_ERROR )
  set( CHECK_CGAL_COMPONENT_ERROR_TITLE  "ERROR:"    )
else()
  if ( NOT CGAL_FIND_QUIETLY )
    set( CHECK_CGAL_COMPONENT_MSG_ON_ERROR TRUE      )
    set( CHECK_CGAL_COMPONENT_ERROR_TYPE   STATUS    )
    set( CHECK_CGAL_COMPONENT_ERROR_TITLE "NOTICE:" )
  else()
    set( CHECK_CGAL_COMPONENT_MSG_ON_ERROR FALSE )
  endif()
endif()

set(CGAL_CONFIGURED_LIBRARIES "CGAL;CGAL_ImageIO")

macro(check_cgal_component COMPONENT)

  set( CGAL_LIB ${COMPONENT} )
  #message("LIB: ${CGAL_LIB}")

  if ( "${CGAL_LIB}" STREQUAL "CGAL" )
    set( CGAL_FOUND TRUE )
    # include CGAL export file
    include(${CGAL_CONFIG_DIR}/CGALExports.cmake)
    # include config file
    include(${CGAL_CONFIG_DIR}/CGALLibConfig.cmake)
    set( CHECK_CGAL_ERROR_TAIL "" )
    get_property(CGAL_CGAL_is_imported TARGET CGAL::CGAL PROPERTY IMPORTED)
    if(CGAL_CGAL_is_imported)
      include("${CGAL_MODULES_DIR}/CGAL_SetupBoost.cmake")
      get_property(CGAL_requires_Boost_libs
        GLOBAL PROPERTY CGAL_requires_Boost_Thread)
      if(CGAL_requires_Boost_libs AND TARGET Boost::thread)
        set_property(TARGET CGAL::CGAL APPEND PROPERTY INTERFACE_LINK_LIBRARIES Boost::thread)
      endif()
    endif()
  else()
    if (EXISTS ${CGAL_CONFIG_DIR}/${CGAL_LIB}Exports.cmake)
      # include export files for requested component
      include(${CGAL_CONFIG_DIR}/${CGAL_LIB}Exports.cmake)
      # include config file (defining WITH_${CGAL_LIB})
      include(${CGAL_CONFIG_DIR}/${CGAL_LIB}LibConfig.cmake)
    endif()

    if ( WITH_${CGAL_LIB} )
      if(TARGET CGAL::${CGAL_LIB})
        if ("${CGAL_LIB}" STREQUAL "CGAL_Qt5")
          
          include("${CGAL_MODULES_DIR}/CGAL_SetupCGAL_Qt5Dependencies.cmake")

          if(CGAL_Qt5_MISSING_DEPS)
            set( CGAL_Qt5_FOUND FALSE )
            message(STATUS "libCGAL_Qt5 is missing the dependencies: ${CGAL_Qt5_MISSING_DEPS} cannot be configured.")
          else()
            set( CGAL_Qt5_FOUND TRUE )
          endif()
        else("${CGAL_LIB}" STREQUAL "CGAL_Qt5")
          # Librairies that have no dependencies
          set( ${CGAL_LIB}_FOUND TRUE )
        endif("${CGAL_LIB}" STREQUAL "CGAL_Qt5")
      else(TARGET CGAL::${CGAL_LIB})
        set( ${CGAL_LIB}_FOUND FALSE )
        set( CHECK_${CGAL_LIB}_ERROR_TAIL " CGAL was configured with WITH_${CGAL_LIB}=ON, but one of the dependencies of ${CGAL_LIB} was not configured properly." )
      endif(TARGET CGAL::${CGAL_LIB})
    else( WITH_${CGAL_LIB} )
      set( ${CGAL_LIB}_FOUND FALSE )
      set( CHECK_${CGAL_LIB}_ERROR_TAIL " Please configure CGAL using WITH_${CGAL_LIB}=ON." )
    endif( WITH_${CGAL_LIB} )
  endif()

  if ( NOT ${CGAL_LIB}_FOUND AND CHECK_CGAL_COMPONENT_MSG_ON_ERROR )
    message( ${CHECK_CGAL_COMPONENT_ERROR_TYPE} "${CHECK_CGAL_COMPONENT_ERROR_TITLE} The ${CGAL_LIB} library was not configured.${CHECK_${CGAL_LIB}_ERROR_TAIL}" )
  endif()

endmacro()

check_cgal_component("CGAL")

foreach( CGAL_COMPONENT ${CGAL_FIND_COMPONENTS} )
  list (FIND CGAL_CONFIGURED_LIBRARIES "CGAL_${CGAL_COMPONENT}" POSITION)
  if ("${POSITION}" STRGREATER "-1") # means: CGAL_COMPONENT is contained in list
    check_cgal_component("CGAL_${CGAL_COMPONENT}")
# TODO EBEB do something for supporting lib in check_component?
  endif()
endforeach()

# Starting with cmake 2.6.3, CGAL_FIND_COMPONENTS is cleared out when find_package returns.
# But we need it within UseCGAL.cmake, so we save it aside into another variable
set( CGAL_REQUESTED_COMPONENTS ${CGAL_FIND_COMPONENTS} )

# for preconfigured libs
set(CGAL_ENABLE_PRECONFIG "ON")
set(CGAL_SUPPORTING_3RD_PARTY_LIBRARIES "GMP;GMPXX;MPFR;ZLIB;OpenGL;LEDA;MPFI;RS;RS3;OpenNL;Eigen3;BLAS;LAPACK;ESBTL;Coin3D;NTL;IPE")
set(CGAL_ESSENTIAL_3RD_PARTY_LIBRARIES "")

set(CGAL_DISABLE_GMP "ON")

include(${CGAL_MODULES_DIR}/CGAL_CreateSingleSourceCGALProgram.cmake)
include(${CGAL_MODULES_DIR}/CGAL_Macros.cmake)

# Temporary? Change the CMAKE module path
cgal_setup_module_path()

if( CGAL_DEV_MODE OR RUNNING_CGAL_AUTO_TEST )
  # Do not use -isystem for CGAL include paths
  set(CMAKE_NO_SYSTEM_FROM_IMPORTED TRUE)
  # Ugly hack to be compatible with current CGAL testsuite process (as of
  # Nov. 2017). -- Laurent Rineau
  include(CGAL_SetupFlags)
endif()
include("${CGAL_MODULES_DIR}/CGAL_enable_end_of_configuration_hook.cmake")
set(CGAL_EXT_LIB_Qt5_PREFIX "QT")
set(CGAL_EXT_LIB_Eigen3_PREFIX "EIGEN3")
set(CGAL_EXT_LIB_Coin3D_PREFIX "COIN3D")
