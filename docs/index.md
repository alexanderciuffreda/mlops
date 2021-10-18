## Gliederung

1. Projektvorstellung
2. Use Case

## Projektvorstellung
Im Rahmen des Projekts wird mit Hilfe von Open-Source-Software ein prototypischer
state-of-the-art Data Lake bzw. eine **Datenplattformarchitektur** für die Realisierung
unterschiedlicher Machine Learning Anwendungsfälle konzipiert und implementiert.

In der Architektur werden für eine möglichst umfassende Automatisierung des **Machine Learning
Lifecycles** Komponenten der Disziplin Machine Learning Operations (MLOps) berücksichtigt.

## Use Case und Datensatz
Um das Projekt umzusetzen, wurde mit dem folgenden Use Case und einem Datensatz der NASA gearbeitet:

Der Datensatz beinhaltet die simulierte Degradation von Turbinen unter verschiedenen Kombinationen
an Betriebsbedinungen und -modi. Des Weiteren werden mehrere Sensorkanäle aufgezeichnet, um die 
Fehlerentwicklung zu charakterisieren./
Es werden Vorhersagen des Zeitpunkts gemacht, zu dem eine Turbine von einer Rakete ihre vorgesehene
Funktion nicht mehr erfüllt. Dafür ist das kontinuierliche Ziel, die Anzahl der verbleibenden Nutzzyklen
des Motors vorherzusagen, die sogenannte Remaining useful life (RUL).
Das binäre Ziel umfasst die Erkennung der letzten 15 Zyklen der Lebensdauer einer Turbine und ob sich
diese darin befindet./
Die Prognose des Ansatzes behandelt dabei jeden Zeitpunkt unabhängig.
