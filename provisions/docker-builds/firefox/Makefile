hostname := firefox-vnc

include ../shared/Makefile.vars
include ../shared/Makefile


firefox:	clean container
	ssh -X $(sshopts) root@$(IP) firefox

