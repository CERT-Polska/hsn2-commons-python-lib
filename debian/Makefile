#
# This Makefile is responsoble for build debian package.
# 4 variables must be provided:
#   HSN2_VER=X.X
#   BUILD_NUMBER=999
#   DEBIAN_DIST=(experimental|2.X)

DEBIAN_DIST=experimental
HSN2LIBPKG=python-hsn2-lib_$(HSN2_VER)-$(BUILD_NUMBER)_all
HSN2TARGETDIR=/opt/hsn2/python/commlib

all: hsn2lib

clean: hsn2lib-clean

hsn2lib: hsn2lib-clean
	mkdir -p $(HSN2LIBPKG)$(HSN2TARGETDIR)
	mkdir -p $(HSN2LIBPKG)/DEBIAN
	cp -r ../*.py $(HSN2LIBPKG)$(HSN2TARGETDIR)
	cp control $(HSN2LIBPKG)/DEBIAN
	sed -i "s/{DEBIAN_DIST}/${DEBIAN_DIST}/" ${HSN2LIBPKG}/DEBIAN/control
	sed -i "s/{VER}/${HSN2_VER}-${BUILD_NUMBER}/" ${HSN2LIBPKG}/DEBIAN/control
	fakeroot dpkg -b $(HSN2LIBPKG)

hsn2lib-clean:
	@rm -rf python-hsn2-lib_*
	
