import nbformat
import subprocess
from jinja2 import Environment, FileSystemLoader
import os
from distutils.dir_util import copy_tree
import shutil

# Configuration variables
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
MVN_TP_PATH = 'spark_app_mvn_project_template'
SCALA_APP_MAIN_PATH = '/src/main/scala/com/stratio/intelligence/App.scala'
SCALA_APP_TP_PATH = MVN_TP_PATH + SCALA_APP_MAIN_PATH
TARGET_DIR = 'target'
SPARK_BIN_PATH='/home/astwin/software/spark-2.1.0-bin-hadoop2.7/bin'

# Auxiliary functions
def remove_folder(path):
    # check if folder exists
    if os.path.exists(path):
        # remove if exists
        shutil.rmtree(path)


# => Remove target directory
remove_folder(TARGET_DIR)


# => Opening notebook
with open('scala_spark_example1.ipynb') as f:
    nb = nbformat.read(f, as_version=4)


# => Extracting notebook code
notebook_code = []
for cell in nb.cells:
    if 'cell_type' in cell and cell.cell_type == 'code':
        if 'source' in cell:
            notebook_code.append(cell.source)
notebook_code = '\n'.join(notebook_code)
print(notebook_code)


# => Filling spark maven templated project
j2_env = Environment(loader=FileSystemLoader(THIS_DIR), trim_blocks=True)

scala_app = j2_env.get_template(SCALA_APP_TP_PATH).render(custom_code=notebook_code)

print(scala_app)

# => Creating new scala maven project
copy_tree(MVN_TP_PATH, 'target')

if os.path.exists(TARGET_DIR+SCALA_APP_MAIN_PATH):
    with open(TARGET_DIR + SCALA_APP_MAIN_PATH, 'w') as f:
        f.write(scala_app)

# => Packaging scala maven project

with subprocess.Popen(['mvn clean package'], shell=True, cwd=TARGET_DIR, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
    for line in p.stdout:
        print(line, end='')  # process line here

# => Executing packaged jar
cmd = SPARK_BIN_PATH + "/spark-submit --master local[8] " 'target/spark_example-1.0-SNAPSHOT-jar-with-dependencies.jar'
with subprocess.Popen(cmd, shell=True, cwd=TARGET_DIR, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
    for line in p.stdout:
        print(line, end='')  # process line here

