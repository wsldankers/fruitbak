pkgsysconfdir = $(sysconfdir)/@PACKAGE@
supportbindir = $(pkgdatadir)/bin
#sbindir = $(exec_prefix)/sbin

bin_SCRIPTS = bin/fruitbak

CLEANFILES = $(bin_SCRIPTS) $(bin_SCRIPTS:=.tmp) $(man_MANS)
EXTRA_DIST = $(bin_SCRIPTS:=.py)

.py:
	mkdir -p $$(dirname $@)
	$(PYTHON3) compile $^ >$@.tmp
	chmod 755 $@.tmp
	mv $@.tmp $@

SUFFIXES = .py .tmp
