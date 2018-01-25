MODULE_big = pgintegrity
OBJS = pgintegrity.o $(WIN32RES)

EXTENSION = pgintegrity
DATA = pgintegrity--0.0.1.sql
PGFILEDESC = "pgintegrity - An integrity checking extension for PostgreSQL"

#REGRESS = pgaudit
#REGRESS_OPTS = --temp-config=$(top_srcdir)/contrib/pgaudit/pgaudit.conf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgintegrity
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
