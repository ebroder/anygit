# TODO: allow this to be overriden by the environment
MONGO_PATCH="../../patches/mongo-python-driver.patch"
GIT_PATCH="../../patches/git-unpack-objects.patch"

install:
	mkdir -p pkgs/lib64/python pkgs/lib/python
	ln -s ../submodules/git pkgs/git
	cd submodules/mongo-python-driver; git apply $(MONGO_PATCH); $(MAKE) $(MFLAGS) install
	cd submodules/git; git apply $(GIT_PATCH); $(MAKE) $(MFLAGS)
clean:
	-cd submodules/mongo-python-driver; $(MAKE) $(MFLAGS) clean
	-cd submodules/git; $(MAKE) $(MFLAGS) clean
	rm -rf pkgs

cert:
	openssl req -new -newkey rsa:2048 -keyout conf/anygit-client.key -out anygit-client.csr
