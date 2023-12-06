import os
import shutil

profile = "default"
plugin_dirname = "OSTranslatorII"

this_dir = os.path.dirname(os.path.realpath(__file__))
home_dir = os.path.expanduser("~")
dest_dir_plug = os.path.join(home_dir, "AppData", "Roaming", "QGIS", "QGIS3", "profiles", profile, "python", "plugins",
                             plugin_dirname)
print(dest_dir_plug)
src_dir_plug = os.path.join(this_dir, plugin_dirname)
try:
    shutil.rmtree(dest_dir_plug)
except OSError:
    pass  # directory doesn't exist
shutil.copytree(src_dir_plug, dest_dir_plug)
