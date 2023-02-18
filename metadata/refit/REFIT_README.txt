REFIT: Electrical Load Measurements

INFORMATION
Collection of this dataset was supported by the Engineering and Physical Sciences Research Council (EPSRC) via the project entitled Personalised Retrofit Decision Support Tools for UK Homes using Smart Home Technology (REFIT), which is a collaboration among the Universities of Strathclyde, Loughborough and East Anglia. The dataset includes data from 20 households from the Loughborough area over the period 2013 - 2014. Additional information about REFIT is available from www.refitsmarthomes.org.

LICENCING
This work is licensed under the Creative Commons Attribution 4.0 International Public License. See https://creativecommons.org/licenses/by/4.0/legalcode for further details.
Please cite the following paper if you use the dataset:

@inbook{278e1df91d22494f9be2adfca2559f92,
title = "A data management platform for personalised real-time energy feedback",
keywords = "smart homes, real-time energy, smart energy meter, energy consumption, Electrical engineering. Electronics Nuclear engineering, Electrical and Electronic Engineering",
author = "David Murray and Jing Liao and Lina Stankovic and Vladimir Stankovic and Richard Hauxwell-Baldwin and Charlie Wilson and Michael Coleman and Tom Kane and Steven Firth",
year = "2015",
booktitle = "Procededings of the 8th International Conference on Energy Efficiency in Domestic Appliances and Lighting",
}

Each of the houses is labelled, House 1 - House 21 (skipping House 14), each house has 10 power sensors comprising a current clamp for the household aggregate and 9 Individual Appliance Monitors (IAMs). Only active power in Watts is collected at 8-second interval.
The subset of all appliances in a household that was monitored reflects the document from DECC of the largest consumers in UK households, https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/274778/9_Domestic_appliances__cooking_and_cooling_equipment.pdf

FILE FORMAT
The file format is csv and is laid out as follows;
UNIX TIMESTAMP (UCT), Aggregate, Appliance1, Appliance2, Appliance3, ... , Appliance9
Additionally data was only recorded when there was a change in load; this data has been filled with intermediate values where not available. The sensors are also not synchronised as our collection script polled every 6-8 seconds; the sensor may have updated anywhere in the last 6 seconds.

MISSING DATA
During the course of the study there are a few periods of missing data (notably February 2014). Outages were due to a number of factors, including household internet failure, hardware failures, network routing issues, etc.

APPLIANCE LIST
The following list shows the appliances that were known to be monitored at the beginning of the study period. Although occupants were asked not to remove or switch appliances monitored by the IAMs, we cannot guarantee this to be the case. It should also be noted that Television and Computer Site may consist of multiple appliances, e.g. Television, SkyBox, DvD Player, Computer, Speakers, etc.

House 1
0.Aggregate, 1.Fridge, 2.Freezer(1), 3.Freezer(2), 4.Washer Dryer,
5.Washing Machine, 6.Dishwasher, 7.Computer, 8.Television Site, 9.Electric Heater

House 2
0.Aggregate, 1.Fridge-Freezer, 2.Washing Machine, 3.Dishwasher, 4.Television Site,
5.Microwave, 6.Toaster, 7.Hi-Fi, 8.Kettle, 9.Overhead Fan

House 3
0.Aggregate, 1.Toaster, 2.Fridge-Freezer, 3.Freezer, 4.Tumble Dryer,
5.Dishwasher, 6.Washing Machine, 7.Television Site, 8.Microwave, 9.Kettle

House 4
0.Aggregate, 1.Fridge, 2.Freezer, 3.Fridge-Freezer, 4.Washing Machine(1),
5.Washing Machine(2), 6.Desktop Computer, 7.Television Site, 8.Microwave, 9.Kettle

House 5
0.Aggregate, 1.Fridge-Freezer, 2.Tumble Dryer 3.Washing Machine, 4.Dishwasher,
5.Desktop Computer, 6.Television Site, 7.Microwave, 8.Kettle, 9.Toaster

House 6
0.Aggregate, 1.Freezer, 2.Washing Machine, 3.Dishwasher, 4.MJY Computer,
5.TV/Satellite, 6.Microwave, 7.Kettle, 8.Toaster, 9.PGM Computer

House 7
0.Aggregate, 1.Fridge, 2.Freezer(1), 3.Freezer(2), 4.Tumble Dryer,
5.Washing Machine, 6.Dishwasher, 7.Television Site, 8.Toaster, 9.Kettle

House 8
0.Aggregate, 1.Fridge, 2.Freezer, 3.Washer Dryer, 4.Washing Machine,
5.Toaster, 6.Computer, 7.Television Site, 8.Microwave, 9.Kettle

House 9
0.Aggregate, 1.Fridge-Freezer, 2.Washer Dryer, 3.Washing Machine, 4.Dishwasher,
5.Television Site, 6.Microwave, 7.Kettle, 8.Hi-Fi, 9.Electric Heater

House 10
0.Aggregate, 1.Magimix(Blender), 2.Toaster, 3.Chest Freezer, 4.Fridge-Freezer,
 5.Washing Machine, 6.Dishwasher, 7.Television Site, 8.Microwave, 9.K Mix

 House 11
 0.Aggregate, 1.Firdge, 2.Fridge-Freezer, 3.Washing Machine, 4.Dishwasher,
 5.Computer Site, 6.Microwave, 7.Kettle, 8.Router, 9.Hi-Fi

 House 12
 0.Aggregate, 1.Fridge-Freezer, 2.???, 3.???, 4.Computer Site,
 5.Microwave, 6.Kettle, 7.Toaster, 8.Television, 9.???

 House 13
 0.Aggregate, 1.Television Site, 2.Freezer, 3.Washing Machine, 4.Dishwasher,
 5.???, 6.Network Site, 7.Microwave, 8.Microwave, 9.Kettle

 House 15
 0.Aggregate, 1.Fridge-Freezer, 2.Tumble Dryer, 3.Washing Machine, 4.Dishwasher,
 5.Computer Site, 6.Television Site, 7.Microwave, 8.Hi-Fi, 9.Toaster

 House 16
 0.Aggregate, 1.Fridge-Freezer(1), 2.Fridge-Freezer(2), 3.Electric Heater(1),
 4.Electric Heater(2), 5.Washing Machine, 6.Dishwasher, 7.Computer Site,
 8.Television Site, 9.Dehumidifier

 House 17
 0.Aggregate, 1.Freezer, 2.Fridge-Freezer, 3.Tumble Dryer, 4.Washing Machine,
 5.Computer Site, 6.Television Site, 7.Microwave, 8.Kettle, 9.TV Site(Bedroom)

 House 18
 0.Aggregate, 1.Fridge(garage), 2.Freezer(garage), 3.Fridge-Freezer,
 4.Washer Dryer(garage), 5.Washing Machine, 6.Dishwasher, 7.Desktop Computer,
 8.Television Site, 9.Microwave

 House 19
 0.Aggregate, 1.Fridge Freezer, 2.Washing Machine, 3.Television Site, 4.Microwave,
 5.Kettle, 6.Toaster, 7.Bread-maker, 8.Games Console, 9.Hi-Fi

 House 20
 0.Aggregate, 1.Fridge, 2.Freezer, 3.Tumble Dryer, 4.Washing Machine, 5.Dishwasher,
 6.Computer Site, 7.Television Site, 8.Microwave, 9.Kettle

 House 21
 0.Aggregate, 1.Fridge-Freezer, 2.Tumble Dryer, 3.Washing Machine, 4.Dishwasher,
 5.Food Mixer, 6.Television, 7.???, 8.Vivarium, 9.Pond Pump