from string import Template
import subprocess
import yaml
import sys

# Read in config
yaml_config = sys.argv[1]

with open(yaml_config, 'r') as f:
    config = yaml.load(f)

MSBuild_path      = config["publish_db"]["MSBuild_path"]
target_database   = config["publish_db"]["target_database"]
connection_string = config["publish_db"]["connection_string"]
template_path     = config["publish_db"]["template_path"]
publish_xml       = config["publish_db"]["publish_xml"]
sqlproj_path      = config["publish_db"]["sqlproj_path"]
publish_template  = {'target_database':target_database,'target_connection_string':connection_string}


def create_publish_xml(template_path=template_path,publish_xml=publish_xml):
    '''
    Creates the XML file required for using MSBuild using a template.
    '''
    try:
        with open(template_path,'r') as f:
            src = Template(f.read())
            new_publish = src.substitute(publish_template)

        with open(publish_xml,'w') as out: #outfilename is config
            for line in new_publish:
                out.write(line)

    except FileNotFoundError as e:
        print(f'XML publish template could not be found. The previous publish XML will be used.')

def _create_msbuild_command(MsBuild_path=MSBuild_path,publish_xml=publish_xml,sqlproj_path=sqlproj_path):
    '''
    Creates the msbuild string for execution
    '''
    cmd = f'"{MSBuild_path}" /t:Clean,Build,Publish /p:SqlPublishProfilePath="{publish_xml}" "{sqlproj_path}"'
    return cmd


def publish(cmd):
    '''
    Calls the MSBuild command.
    '''
    subprocess.check_call(cmd, shell=False)


def main():
    create_publish_xml()
    publish(_create_msbuild_command())

if __name__ == '__main__':
    main()
