PLUGIN_NAME := hyprland_windows
PLUGIN_DIR := $(CURDIR)
INSTALL_ROOT := $(HOME)/.local/lib/tarragon/plugins/$(PLUGIN_NAME)

.PHONY: check-deps install uninstall run test

check-deps:
	@command -v python3 >/dev/null 2>&1 || { echo "python3 not found" >&2; exit 1; }
	@command -v hyprctl >/dev/null 2>&1 || { echo "hyprctl not found" >&2; exit 1; }
	@echo "Dependencies OK"

install: check-deps
	@mkdir -p "$(INSTALL_ROOT)"
	@install -m 0644 "$(PLUGIN_DIR)/plugin.toml" "$(INSTALL_ROOT)/plugin.toml"
	@install -m 0755 "$(PLUGIN_DIR)/hyprland_windows_plugin.py" "$(INSTALL_ROOT)/hyprland_windows_plugin.py"
	@install -m 0644 "$(PLUGIN_DIR)/README.md" "$(INSTALL_ROOT)/README.md"

uninstall:
	@rm -rf "$(INSTALL_ROOT)"

run:
	@python3 "$(PLUGIN_DIR)/hyprland_windows_plugin.py" --once ""

test:
	@PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$(PLUGIN_DIR)" -p 'test_*.py'
