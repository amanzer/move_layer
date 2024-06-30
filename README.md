# Move_layer

This script is entirely based on the Move plugin built by Maxime Schoemans

https://github.com/mschoema/move



## Installation steps

Installing the plugin is done locally using the [Plugin Builder Tool](http://g-sherman.github.io/plugin_build_tool/).

 1. Clone the [Move github repository](https://github.com/mschoema/move) (or download and unzip the project) in the directory of your choice.
 2. Install the [Plugin Builder Tool](http://g-sherman.github.io/plugin_build_tool/).
 3. Open a terminal in the [move](https://github.com/mschoema/move/tree/master/move) directory and run the following command:
```shell
pbt deploy -y
```
 4. In QGIS, go to Plugins->Manage and Install Plugins->Installed, and check the box next to the plugin.
![Manage and Install Plugin](img/manage_and_install_plugins.png "Install Move plugin")


## Using the plugin

### Plugin interface

The plugin has a simple interface that can be opened using Database->Move->Open Move Interface, or using the button in the top toolbar.

When opened, the plugin is displayed as a widget at the bottom of the QGIS window, and it has 4 elements:

 1. A combobox to select the database to use.
 2. A textbox to write SQL SELECT queries.
 3. An *Execute Query* button.
 4. A *Refresh Layers* button.

![Plugin Interface](img/plugin_interface.png "Plugin Interface")

### Database connection

The plugin detects existing PostGIS database connections, and lists them in its combobox.  
When executing a query, the plugin will use the database connection currently selected in the combobox.  
To refresh the available databases, simply close and re-open the plugin.

#### Database requirements

To work correctly, the plugin requires these database connections to have their username and password stored.  
Additionally, to handle the database backend correctly, the plugin needs to be run in a project with a defined title.  
To define the title of the project, go to Project->Properties->General->Project Title.

### Execute Query




#### PostGIS geometries


#### MobilityDB temporal points



### Refresh Layers



## Issues and ideas

For any issues or improvement ideas, open a new git issue or send an email to maxime.schoemans@ulb.be

## License

Move is licensed under the terms of the MIT License (see the file
[LICENSE](https://github.com/mschoema/move/blob/master/LICENSE)).
