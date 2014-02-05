celery-sopnet
=============

A Python module to provide Celery tasks for Sopnet.

Usage
=====

A Celery server that should be able to execute tasks of this module has to know
them. The included celery sub-module creates a Celery instance and can be run as
a stand-alone celery server. To do this, the following command can be used
(assuming a locally running RabbitMQ Server):

    celery worker --app=celerysopnet --broker='amqp://guest@localhost//' -l info
