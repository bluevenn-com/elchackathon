Watch App
---------
The project has been developed using the Garmin Connect IQ SDK, for all supported devices.  Developed in the Garmin proprietary ‘Monkey C’ language (mc file extension), the source code was produced within the Eclipse IDE.

The Watch Application, has an application class (IQAppELCHackathonApp.mc) which launches a View and Behaviour/Input controller (IQAppELCHackathonView.mc).

The View:
- Enables the device sensors (Heart Rate, Blood Oxygen Sp02, Temperature) to collect the biometric data.  
- Renders a number of screens to display the data, and alerts to the user.
- Sends the data to an AWS web service (project included in repository).

Listener
--------
The two lambda functions run within AWS.  

Data is uploaded to AWS from the watch using an API Gateway HTTP endpoint whihc pushes data into an SQS queue.
The ListenerLambda then takes data from this SQS queue and stores it in an Aurora Serverless Database.

Data is fetched into our application from a second API Gateway HTTP endpoint.  This calls the FetchLambda to service the request.
The FetchLamda takes a paged block of rows from the Aurora Database, packages it up as a JSON array and returns it to the application.

The main processing within our application runs in the ExternalTableManager and the ExternalTable objects.  These 2 classes were added to our large existing commercial application to support this Hackathon.  Essentially the manager queues up the ExternalTable object to periodically call the FetchLambda.  The data retrieved is then applied a mapping and loaded into a table within our platform.

From there we created some addditional campaigns and emails to monitor these event feeds and send out apporpriate emails to the physician and the patient.

Attached are the core files added to our various tiers to support this concept.

Thanks

BlueVenn and Estee Lauder Hackathon Team
