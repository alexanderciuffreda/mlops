## Gliederung

1. Projektvorstellung
2. Use Case

# Projektvorstellung
Im Rahmen des Projekts wird mit Hilfe von Open-Source-Software ein prototypischer\
state-of-the-art Data Lake bzw. eine **Datenplattformarchitektur** für die Realisierung\
unterschiedlicher Machine Learning Anwendungsfälle konzipiert und implementiert.

In der Architektur werden für eine möglichst umfassende Automatisierung des **Machine Learning
Lifecycles** Komponenten der Disziplin Machine Learning Operations (MLOps) berücksichtigt.

# Use Case und Datensatz
Um das Projekt umzusetzen wurde mit dem folgenden Use Case und einem Datensatz der NASA gearbeitet:

Der Datensatz beinhaltet die simulierte Motordegradation von Turbinen unter verschiedenen Kombinationen
von betriebsbedinungen und -modi. Es werden mehrere Sensorkanäle aufgezeichnet, um die Fehlerentwicklung
zu charakterisieren.
Es werden Vorhersagen des Zeitpunkts gemacht, zu dem eine Turbine von einer Rakete ihre Vorgesehene
Funktion nicht mehr erfüllt. Dafür ist das kontinuiertliche Ziel, die Anzahl der verbleibenden Nutzzyklen
des Motors vorherzusagen, die sogenannte Remaining useful life (RUL).
Das binäre Ziel umfasst die Erkennung, dass die Turbine sich in den letzen 15 Zyklen ihrer Lebensdauer befindet.
Die Prognose des Ansatzes behandelt dabei jeden Zeitpunkt unabhängig.
