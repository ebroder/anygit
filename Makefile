install:
	mkdir -p pkgs/lib64/python
	cd submodules/mongo-python-driver; git apply ../../patches/mongo-python-driver.patch; $(MAKE) $(MFLAGS) install

clean:
	-cd submodules/mongo-python-driver; $(MAKE) $(MFLAGS) clean
	rm -rf pkgs
