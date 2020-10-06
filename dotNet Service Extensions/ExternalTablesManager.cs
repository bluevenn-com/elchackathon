//-----------------------------------------------------------------------------
// © 2020 BlueVenn Limited
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.DataManagement.Shared
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	01/10/2020	J.Boyce					First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BlueVenn.Indigo.Core;
using BlueVenn.Indigo.Core.Async;
using BlueVenn.Indigo.Core.Logging;
using BlueVenn.Indigo.Core.Resources;
using BlueVenn.Indigo.DataManagement.Shared.Interfaces;
using BlueVenn.Indigo.DataManagement.Shared.Resources;
using BlueVenn.Indigo.ObjectManagement.Shared.Interfaces;
using BlueVenn.Indigo.ServerManagement.Shared.Interfaces;
using Ninject;

namespace BlueVenn.Indigo.DataManagement.Server
{
	/// <summary>
	/// The external tables manager singleton
	/// </summary>
	/// <remarks>
	/// The external tables manager is a singleton used to auto update a table based on
	/// the existence of a
	/// </remarks>
	public class ExternalTablesManager : IExternalTablesManager
	{
		/// <summary>
		/// Helper to get the object manager
		/// </summary>
		public IObjectManager ObjectManager
		{
			get
			{
				// Get the object manager from the DC
				return GlobalDC.Container.Get<IObjectManager>();
			}
		}

		/// <summary>
		/// Helper to get the data manager
		/// </summary>
		public IDataManager DataManager
		{
			get
			{
				// Get the data manager from the DC
				return GlobalDC.Container.Get<IDataManager>();
			}
		}

		/// <summary>
		/// The list of asynch tasks running and auto updating external tables with data from listeners
		/// </summary>
		private Dictionary<int, ClientTask> ExternalTableAutoUpdateTasks { get; set; }

		/// <summary>
		/// Initializes a new instance of the ExternalTablesManager class.
		/// </summary>
		/// <remarks>
		/// The default constructor initializes any fields to their default values.
		/// </remarks>		
		public ExternalTablesManager()
		{
			// Initialise members
			ExternalTableAutoUpdateTasks = new Dictionary<int, ClientTask>();
		}

		/// <summary>
		/// Initialise the ExternalTablesManager
		/// </summary>
		/// <remarks>Performs initialisation of the ExternalTablesManager. This method is called just once during service startup.
		/// </remarks>
		public void Initialise()
		{
			// Find all external tables
			FilterCollection filterCol = new FilterCollection();
			ObjectManagementFilter omFilter = new ObjectManagementFilter { ObjectType = DataManagementObjectTypesAndGroups.ExternalTable };
			filterCol.Add( omFilter );
			List<IObjectAttributes> allExternalTables = ObjectManager.List( filterCol );

			// Find external tables which have an autoupdate and queue auto-update tasks against them
			foreach( IObjectAttributes currET in allExternalTables )
				QueueAutoUpdateTask( currET.Id );

			// Add an event listener for object notifications - we are only interested in add/change/delete of external tables
			ObjectManager.RegisterObjectNotificationListener( new ObjectNotificationListener { EventTypes = EObjectNotificationEventTypes.ObjectModification | EObjectNotificationEventTypes.ObjectCreation | EObjectNotificationEventTypes.ObjectModification, 
				ObjectTypeIds = new List<int> { DataManagementObjectTypesAndGroups.ExternalTable }, OnObjectNotification = OnObjectNotification } );
		}

		/// <summary>
		/// Called at start up and each time an auto-update completes - starts next auto update
		/// </summary>
		/// <remarks>
		/// Fully checks the external table is appropriate before adding to auto-update queue
		/// </remarks>
		/// <param name="externalTableId">The external table to check</param>
		/// <returns>Client task running the job or null if not appropriate</returns>
		private ClientTask QueueAutoUpdateTask( int externalTableId )
		{
			// Check and create task params (if this is auto-updateable)
			TaskParameters taskParams = CheckAndConfigureAutoUpdateableTableTaskParams( externalTableId );
			if( taskParams == null )
				return null;
			string externalTableName = taskParams.GetValue<string>( "externalTableName" );
			string listenerName = taskParams.GetValue<string>( "listenerName" );
			ClientTask taskWrapper = new ClientTask();
			taskWrapper.ExecuteTaskWrapper( externalTableName, "LoadExternalTableListenerData", StringTables.Messages.RES_AUTOUPDATINGEXTERNALTABLEFROMLISTENER( externalTableName, listenerName ), new List<Action<TaskParameters>> { AutoUpdateExternalTable }, taskParams, AutoUpdateFinished );

			// Add this running task to our dictionary and return it
			lock( ExternalTableAutoUpdateTasks )
				ExternalTableAutoUpdateTasks.Add( externalTableId, taskWrapper );
			return taskWrapper;
		}

		/// <summary>
		/// Checks this external table is auto-updateable and valid to use
		/// </summary>
		/// <param name="externalTableId">The external table to check</param>
		/// <returns>Full set up task params if external table is appropriate, otherwise null</returns>
		private TaskParameters CheckAndConfigureAutoUpdateableTableTaskParams( int externalTableId )
		{
			// Check the external table is fundamentally valid to be auto-updated
			IObjectAttributes currET = ObjectManager.GetObjectAttributes( externalTableId );
			if( currET == null || currET.IsDeleted || !currET.IsValid || currET.IsShortcut || currET.IsTemplate || currET.IsPlaceholder )
				return null;

			// Get the full object and check it has an autoupdate source
			IExternalTable currETObj = DataManager.GetExternalTable( currET, false );
			if( currETObj == null || currETObj.AutoUpdateSource == null || currETObj.AutoUpdateSource.Id < 1 || !currETObj.Interval.HasValue || currETObj.Interval.Value < 1 )
				return null;

			// Get the listener (just to put its name in the task log - all further validation done inside the ExternalTable:AutoUpdate as we want to log if this isnt set right)
			IObjectAttributes currListener = ObjectManager.GetObjectAttributes( currETObj.AutoUpdateSource.Id );
			string listenerName = currListener?.Name ?? SharedStringTables.Shared.RES_UNKNOWN;

			// Create all the task parameters 
			TaskParameters taskParams = new TaskParameters();
			taskParams.SetValue( "taskName", "LoadExternalTableListenerData" );
			taskParams.SetValue( "externalTableId", currET.Id );
			taskParams.SetValue( "externalTableName", currET.Name );
			taskParams.SetValue( "listenerId", currETObj.AutoUpdateSource.Id );
			taskParams.SetValue( "listenerName", listenerName );
			taskParams.SetValue( "autoUpdateInterval", currETObj.Interval.Value );

			// Add a special delay param.  This tells task wrapper in ClientTask to delay this many ms before starting the first task item
			taskParams.SetValue( "__DelayedExecution__", currETObj.Interval.Value * 60000 );
			return taskParams;
		}

		/// <summary>
		/// Threaded task to build initial counts for a level
		/// </summary>
		/// <param name="taskParams">The specific task instances parameters</param>
		private void DelayAutoUpdateExternalTable( TaskParameters taskParams )
		{
			if( taskParams.GetValue<int>( "autoUpdateInterval" )  > 0 )
				Task.Delay( taskParams.GetValue<int>( "autoUpdateInterval" ) ).Wait();		
		}

		/// <summary>
		/// Threaded task to build initial counts for a level
		/// </summary>
		/// <param name="taskParams">The specific task instances parameters</param>
		private void AutoUpdateExternalTable( TaskParameters taskParams )
		{
			// Recheck all is apporpriate still after the delay (we dont use these params, just make sure not null!)
			int externalTableId = taskParams.GetValue<int>( "externalTableId" );
			string externalTableName = taskParams.GetValue<string>( "externalTableName" );
			string listenerName = taskParams.GetValue<string>( "listenerName" );
			int taskRunId = taskParams.GetValue<int>( "__TaskRunId__" );
			TaskParameters chkParams = CheckAndConfigureAutoUpdateableTableTaskParams( externalTableId );
			int numRows = 0;
			if( chkParams != null )
			{
				// Call the external table object itself to do the work
				IExternalTable currETObj = ObjectManager.GetObject( externalTableId ) as IExternalTable;
				if( currETObj != null )
					numRows = currETObj.AutoUpdate();
			}

			// Build all counts for all campaigns at this level
			ExternalTableAutoUpdateExecutionInfo execInfo = new ExternalTableAutoUpdateExecutionInfo { ExternalTableName = externalTableName, ExternalResourceName = listenerName, NumRecords = numRows };

			// Write a task run detail so we can see how long this takes and how much we counted
			TaskRunDetail taskRunDetail = new TaskRunDetail();
			taskRunDetail.SubItemId = 1;
			taskRunDetail.Status = (int)( EProgressState.Stopped | EProgressState.Successful );
			taskRunDetail.StartTime = DateTime.UtcNow;
			taskRunDetail.EndTime = DateTime.UtcNow;
			taskRunDetail.Details = execInfo;
			GlobalDC.Container.Get<IServerManager>().GetActivityLogStore().CreateTaskRunDetail( taskRunId, taskRunDetail );
			GlobalDC.Container.Get<ILogger>().Log( execInfo.GetMessage() );		
		}

		/// <summary>
		/// Called at the end of the auto update - checks and queues another call
		/// </summary>
		/// <param name="taskParams">The specific task instances parameters</param>
		private void AutoUpdateFinished( TaskParameters taskParams )
		{
			// Get the finished task and remove from the dictionary
			int externalTableId = taskParams.GetValue<int>( "externalTableId" );
			lock( ExternalTableAutoUpdateTasks )
				if( ExternalTableAutoUpdateTasks.ContainsKey( externalTableId ) )
					ExternalTableAutoUpdateTasks.Remove( externalTableId );

			// And try to start another one
			QueueAutoUpdateTask( externalTableId );
		}

		/// <summary>
		/// Event callback for when an object notification has been received
		/// </summary>
		/// <remarks>
		/// Listens for changes to external tables to make changes to autoupdate tasks
		/// </remarks>
		/// <param name="sender">The object making this call</param>
		/// <param name="args">The object notification event args</param>
		private void OnObjectNotification( object sender, ObjectNotificationEventArgs args )			
		{
			// TODO - currently only config'd at service start
		}
	}
}
