cmake_minimum_required(VERSION 3.18)
project(daggerml LANGUAGES C)

option(DML_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(DML_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

set(_dml_sanitize_flags "")
if(DML_ENABLE_ASAN)
  list(APPEND _dml_sanitize_flags "-fsanitize=address")
endif()
if(DML_ENABLE_UBSAN)
  list(APPEND _dml_sanitize_flags "-fsanitize=undefined")
endif()
if(_dml_sanitize_flags)
  list(APPEND _dml_sanitize_flags "-fno-omit-frame-pointer")
endif()

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)

add_library(lmdb STATIC
  c/third_party/lmdb/libraries/liblmdb/mdb.c
  c/third_party/lmdb/libraries/liblmdb/midl.c
)
target_include_directories(lmdb PUBLIC c/third_party/lmdb/libraries/liblmdb)
set_property(TARGET lmdb PROPERTY POSITION_INDEPENDENT_CODE ON)
if(_dml_sanitize_flags)
  target_compile_options(lmdb PRIVATE ${_dml_sanitize_flags})
  target_link_options(lmdb PRIVATE ${_dml_sanitize_flags})
endif()

add_library(msgpackc STATIC
  c/third_party/msgpack/src/objectc.c
  c/third_party/msgpack/src/unpack.c
  c/third_party/msgpack/src/vrefbuffer.c
  c/third_party/msgpack/src/zone.c
  c/third_party/msgpack/src/version.c
)
target_include_directories(msgpackc PUBLIC c/third_party/msgpack/include)
set_property(TARGET msgpackc PROPERTY POSITION_INDEPENDENT_CODE ON)
if(_dml_sanitize_flags)
  target_compile_options(msgpackc PRIVATE ${_dml_sanitize_flags})
  target_link_options(msgpackc PRIVATE ${_dml_sanitize_flags})
endif()

add_library(sha256 STATIC
  c/third_party/sha256/sha256.c
)
target_include_directories(sha256 PUBLIC c/third_party/sha256)
set_property(TARGET sha256 PROPERTY POSITION_INDEPENDENT_CODE ON)
if(_dml_sanitize_flags)
  target_compile_options(sha256 PRIVATE ${_dml_sanitize_flags})
  target_link_options(sha256 PRIVATE ${_dml_sanitize_flags})
endif()

add_library(daggerml_core STATIC
  c/src/dml_db.c
  c/src/dml_hash.c
  c/src/dml_msgpack.c
  c/src/dml_value.c
)
target_include_directories(daggerml_core PUBLIC c/include)
target_include_directories(daggerml_core PUBLIC c/third_party/msgpack/include)
set_property(TARGET daggerml_core PROPERTY POSITION_INDEPENDENT_CODE ON)
if(_dml_sanitize_flags)
  target_compile_options(daggerml_core PRIVATE ${_dml_sanitize_flags})
  target_link_options(daggerml_core PRIVATE ${_dml_sanitize_flags})
endif()

set(core_db_pyx ${CMAKE_CURRENT_SOURCE_DIR}/src/daggerml/_core/db.pyx)
set(core_db_c ${CMAKE_CURRENT_BINARY_DIR}/core_db.c)

add_custom_command(
  OUTPUT ${core_db_c}
  COMMAND ${Python_EXECUTABLE} -m cython -3 --output-file ${core_db_c} ${core_db_pyx}
  DEPENDS ${core_db_pyx}
  COMMENT "Cythonizing db.pyx"
  VERBATIM
)

add_library(db MODULE
  ${core_db_c}
)
set(CORE_DB_EXPORTS "${CMAKE_CURRENT_BINARY_DIR}/core_db.exports")
if(APPLE)
  file(WRITE "${CORE_DB_EXPORTS}" "_PyInit_db\n")
  target_link_options(db PRIVATE
    "-Wl,-exported_symbols_list,${CORE_DB_EXPORTS}"
  )
else()
  file(WRITE "${CORE_DB_EXPORTS}" "{_PyInit_db;};")
  target_link_options(db PRIVATE
    "-Wl,--version-script=${CORE_DB_EXPORTS}"
  )
endif()
target_include_directories(db PRIVATE c/third_party/lmdb/libraries/liblmdb)
target_include_directories(db PRIVATE c/third_party/msgpack/include)
target_include_directories(db PRIVATE c/include)
target_link_libraries(db PRIVATE daggerml_core lmdb msgpackc sha256 Python::Module)
if(_dml_sanitize_flags)
  target_compile_options(db PRIVATE ${_dml_sanitize_flags})
  target_link_options(db PRIVATE ${_dml_sanitize_flags})
  if(DML_ENABLE_ASAN AND NOT APPLE AND CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_link_options(db PRIVATE "-shared-libasan")
  endif()
  if(DML_ENABLE_UBSAN AND CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(db PRIVATE "-fno-sanitize=function")
  endif()
endif()

set_target_properties(db PROPERTIES PREFIX "" OUTPUT_NAME "db")

install(TARGETS db
  LIBRARY DESTINATION daggerml/_core/
  RUNTIME DESTINATION daggerml/_core/
  ARCHIVE DESTINATION daggerml/_core/
)
