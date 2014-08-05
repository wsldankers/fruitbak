AC_INIT([fruitbak], [0.0])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-xz])

AC_CONFIG_SRCDIR(bin/fruitbak.pl)

AC_SUBST([pkglocalstatedir], ['${localstatedir}/lib/${PACKAGE}'])
AC_SUBST([pkgsysconfdir], ['${sysconfdir}/${PACKAGE}'])
AC_SUBST([pkgdatadir], ['${datadir}/${PACKAGE}'])
AC_SUBST([pkglibdir], ['${libdir}/${PACKAGE}'])

AC_PROG_INSTALL

AC_PATH_PROG(PERL, perl)
case $PERL in '')
	AC_MSG_ERROR([cannot find perl in your PATH.])
esac

AC_CONFIG_COMMANDS([config.perl],[echo "$allconfigvars" >config.perl],[
allconfigvars=\$(cat <<'__CONFIGVARS_EOT'
$(for ax_var in $ac_subst_vars
do
	eval echo "$ax_var=\$$ax_var"
done)
__CONFIGVARS_EOT
)])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
