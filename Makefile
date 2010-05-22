# TODO: allow this to be overriden by the environment
MONGO_PATCH="../../patches/mongo-python-driver.patch"

install:
	mkdir -p pkgs/lib64/python
	cd submodules/mongo-python-driver; git apply $(MONGO_PATCH); $(MAKE) $(MFLAGS) install

clean:
	-cd submodules/mongo-python-driver; $(MAKE) $(MFLAGS) clean
	rm -rf pkgs

cert:
	openssl req -new -newkey rsa:2048 -keyout conf/anygit-client.key -out anygit-client.csr
