pkgsysconfdir = $(sysconfdir)/@PACKAGE@
scriptdir = $(pkgdatadir)/bin

bin_SCRIPTS = bin/fruitbak
script_SCRIPTS = bin/fruitbak-rsyncp-recv
man_MANS = $(bin_SCRIPTS:=.1)

CLEANFILES = $(bin_SCRIPTS) $(bin_SCRIPTS:=.tmp) $(man_MANS)
EXTRA_DIST = $(bin_SCRIPTS:=.pl)

.pl:
	mkdir -p $$(dirname $@)
	$(PERL) compile $^ >$@.tmp
	chmod 755 $@.tmp
	mv $@.tmp $@

.pl.1:
	mkdir -p $$(dirname $@)
	pod2man --utf8 $< $@

SUFFIXES = .pl .1
