PLUGIN_NAME := ports
PLUGIN_DIR := $(CURDIR)
INSTALL_ROOT ?= $(HOME)/.local/lib/tarragon/plugins/$(PLUGIN_NAME)

.PHONY: check-deps install uninstall run

check-deps:
	@command -v python3 >/dev/null 2>&1 || { echo "python3 not found" >&2; exit 1; }
	@python3 -c 'import sys; print("python", sys.version.split()[0])' || { echo "python3 check failed" >&2; exit 1; }
	@for p in ss lsof /usr/sbin/ss /sbin/ss; do \
		if command -v "$$p" >/dev/null 2>&1 || [ -x "$$p" ]; then \
			echo "socket tool OK ($$p)"; exit 0; \
		fi; \
	done; \
	echo "neither 'ss' (iproute2) nor 'lsof' found" >&2; exit 1

install: check-deps
	@echo "Installing $(PLUGIN_NAME) plugin to $(INSTALL_ROOT)"
	@mkdir -p $(INSTALL_ROOT)
	@install -m 0644 plugin.toml $(INSTALL_ROOT)/plugin.toml
	@install -m 0755 ports_plugin.py $(INSTALL_ROOT)/ports_plugin.py
	@install -m 0644 README.md $(INSTALL_ROOT)/README.md
	@echo "Installed."

uninstall:
	@echo "Removing $(INSTALL_ROOT)"
	@rm -rf "$(INSTALL_ROOT)"

run:
	@python3 $(PLUGIN_DIR)/ports_plugin.py tarragon query ""
