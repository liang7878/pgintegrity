EXTENSION = pgintegrity              # the extension name
DATA      = pgintegrity--0.0.1.sql   # script files to install
#REGRESS   = base36_test         # our test script file (without extension)
MODULES   = pgintegrity              # our c module file to build

# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
