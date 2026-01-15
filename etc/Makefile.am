#if HAVE_SYSTEMD
systemdsystemunit_DATA = flux-accounting.service
#endif

noinst_SCRIPTS = \
	completions/get_accounting_builtins.sh \
	completions/flux_accounting.pre

completions/flux_accounting: completions/flux_accounting.pre completions/get_accounting_builtins.sh
	$(AM_V_GEN)test -d completions || mkdir completions && \
	cp $< $@ && chmod +w $@ && \
	$(srcdir)/completions/get_accounting_builtins.sh \
	$(top_srcdir)/src/cmd/builtin >> $@

bashcomp_SCRIPTS = \
	completions/flux_accounting
