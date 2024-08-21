


from services.version_maintainer import VersionManager


check  = VersionManager("cell_changes")

print(check.apply_new_changes("demo10_20240820_141456"))