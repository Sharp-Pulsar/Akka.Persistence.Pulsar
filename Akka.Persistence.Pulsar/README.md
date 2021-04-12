# Akka.Persistence.Pulsar

Akka Persistence Pulsar Plugin is a plugin for `Akka persistence` that provides several components:
 - a journal store ;
 - a snapshot store ;
 - a journal query interface implementation.

This plugin stores data in a [pulsar](https://pulsar.apache.org/) ledger and based on [SharpPulsar](https://github.com/eaba/SharpPulsar) library. 