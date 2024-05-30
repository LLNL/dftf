Drink From The Firehose (DFTF) is a python program that subscribes to
Redfish events on Cray/HPE hardware, and republishes them to topics
in Kafka. Additionally, it handles Slingshot Telemetry messages with
a MessageID prefix of CrayFabricHealth.

DFTF will create subscriptions for two types of Redfish events:

- CrayTelemetry Alerts
- Normal (non-CrayTelemetry) Alerts

Slingshot telemetry must be configured separately using slingshot
CLI tools.

CrayTelemetry alerts are sent at regular intervals (typically 5 seconds)
decided upon by the Redfish server. The OEM field of the event contains
a large amount of metric data formated in a nonstandard (OEM decided)
Cray format. Typically the event message consolidates metric samples
taken at one second intervals.

In an attempt to tame the firehouse of information from CrayTelmetry,
DFTF drops any repeated metrics, so only the most recent value for
each unique metric is maintained. In effect this usually means values
are reported roughly every five seconds rather than every second.

Topic names for Kafka have a common configurable prefix. The rest of
the topic name for the CrayTelemetry data is "craytelemetry", and the
rest of the topic name for normal Alerts is "crayevents".

See the example.conf file for example configuration.

In additon to the "general" section in the configuration file, there
may be sections named after the hostnames of the nodes running DFTF.
This way, a single configuration file may be shared amongst all
DFTF daemons, if desired, and the daemon will only subsribe to the
redfish servers listed in its namesake section.

License
----------------

SPDX-License-Identifier: BSD-3-Clause

LLNL-CODE-849577
