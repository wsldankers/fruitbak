pkgsysconfdir = $(sysconfdir)/@PACKAGE@
supportbindir = $(pkgdatadir)/bin
#sbindir = $(exec_prefix)/sbin

bin_SCRIPTS = bin/fruitbak
supportbin_SCRIPTS = bin/fruitbak-rsyncp-recv
#man_MANS = $(bin_SCRIPTS:=.1)

CLEANFILES = $(bin_SCRIPTS) $(bin_SCRIPTS:=.tmp) $(supportbin_SCRIPTS) $(supportbin_SCRIPTS:=.tmp) $(man_MANS)
EXTRA_DIST = $(bin_SCRIPTS:=.py) $(supportbin_SCRIPTS:=.pl)

.py:
	mkdir -p $$(dirname $@)
	$(PERL) compile python $^ >$@.tmp
	chmod 755 $@.tmp
	mv $@.tmp $@

.pl:
	mkdir -p $$(dirname $@)
	$(PERL) compile perl $^ >$@.tmp
	chmod 755 $@.tmp
	mv $@.tmp $@

.pl.1:
	mkdir -p $$(dirname $@)
	pod2man --utf8 $< $@

SUFFIXES = .py .pl .1 .tmp
