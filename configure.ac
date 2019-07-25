AC_INIT([fruitbak], m4_esyscmd_s([
	read -r debian_version <debian/changelog
	debian_version=${debian_version#*\(}
	debian_version=${debian_version%%\)*}
	echo "${debian_version%-*}"
]))
AM_INIT_AUTOMAKE([foreign subdir-objects dist-xz no-dist-gzip])

AC_CONFIG_SRCDIR(bin/fruitbak.py)

AC_SUBST([pkglocalstatedir], ['${localstatedir}/lib/${PACKAGE}'])
AC_SUBST([pkgsysconfdir], ['${sysconfdir}/${PACKAGE}'])
AC_SUBST([pkgdatadir], ['${datadir}/${PACKAGE}'])
AC_SUBST([pkglibdir], ['${libdir}/${PACKAGE}'])

AC_PROG_INSTALL

AC_CACHE_CHECK([for the full path of Python 3], [ac_cv_path_PYTHON], [
	AC_PATH_PROGS_FEATURE_CHECK([PYTHON], [python python3], [
		case $($ac_path_PYTHON -V 2>/dev/null) in Python\ 3.*)
			ac_cv_path_PYTHON=$ac_path_PYTHON
			ac_path_PYTHON_found=:
		esac
	], [AC_MSG_ERROR([cannot find python 3 in your PATH.])])
])
AC_SUBST([PYTHON], [$ac_cv_path_PYTHON])

# for the compile script
AC_PATH_PROG(PERL, perl)
case $PERL in '')
	AC_MSG_ERROR([cannot find perl in your PATH.])
esac

AC_CONFIG_COMMANDS([scriptconfig], [echo "$allconfigvars" >scriptconfig], [
allconfigvars=\$(cat <<'__CONFIGVARS_EOT'
$(for ax_var in $ac_subst_vars
do
	eval echo "$ax_var=\$$ax_var"
done)
__CONFIGVARS_EOT
)])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
