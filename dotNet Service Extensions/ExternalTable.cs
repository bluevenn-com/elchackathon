//-----------------------------------------------------------------------------
// © 2020 BlueVenn Ltd
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.DataManagement.Server
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	06/10/2020	J.Boyce 	-			First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using BlueVenn.Indigo.CampaignManagement.Shared.Interfaces;
using BlueVenn.Indigo.Core;
using BlueVenn.Indigo.Core.Async;
using BlueVenn.Indigo.Core.Configuration;
using BlueVenn.Indigo.Core.Logging;
using BlueVenn.Indigo.Core.Resources;
using BlueVenn.Indigo.Core.Security;
using BlueVenn.Indigo.DataAccess.Shared;
using BlueVenn.Indigo.DataAccess.Shared.Interfaces;
using BlueVenn.Indigo.DataAccess.Shared.Schema;
using BlueVenn.Indigo.DataManagement.Server.Service.Sync;
using BlueVenn.Indigo.DataManagement.Shared.Interfaces;
using BlueVenn.Indigo.DataManagement.Shared.Resources;
using BlueVenn.Indigo.MetadataManagement.Shared.Interfaces;
using BlueVenn.Indigo.ObjectManagement.Shared.Interfaces;
using BlueVenn.Indigo.ServerManagement.Shared.Interfaces;
using Newtonsoft.Json;
using Ninject;

namespace BlueVenn.Indigo.DataManagement.Server.Objects
{
	/// <summary>
	/// Describes an external table object
	/// </summary>
	/// <remarks>
	/// External table objects allow customers to load bespoke data into their 
	/// analytic store in order to extend it
	/// </remarks>
	public class ExternalTable : ExternalMappingObject, IExternalTable
	{
		/// <summary>
		/// Contains a queue of tasks to notify assoicated objects of changes
		/// </summary>
		private ITaskQueue m_TaskQueue;

		/// <summary>
		/// Async data processing lock object
		/// </summary>
		/// <remarks>
		/// Used with a lock to prevent multiple threads updating the table at the same time
		/// </remarks>
		private readonly object m_DataProcessLock = new object();

		/// <summary>
		/// Constructor
		/// </summary>
		public ExternalTable() : base()
		{
			Table = new ObjectIdentity();

			// Set up task queue to handle associated object notifications
			// Only allow 1 task to run at a time to ensure notifications are sent in series
			m_TaskQueue = new TaskQueue( 1, true );
		}

		#region IExternalTable

		/// <summary>
		/// Gets or sets the table this external table is loading data into
		/// </summary>
		public ObjectIdentity Table { get; set; }

		/// <summary>
		/// Gets or sets the table type
		/// </summary>
		public EExternalTableType TableType { get; set; }

		/// <summary>
		/// Gets or sets the external resource object to use as an auto update source
		/// </summary>
		public ObjectIdentity AutoUpdateSource { get; set; }

		/// <summary>
		/// Gets or sets the external resource object auto update source interval in minutes
		/// </summary>
		/// <remarks>
		/// Only applicable if an auto update source has been defined
		/// </remarks>
		public int? Interval { get; set; }

		/// <summary>
		/// The last auto update event fetched by this external tables
		/// </summary>
		public int? LastFetchedAutoUpdateEventId { get; set; }

		/// <summary>
		/// Gets a flag which indicates if the external table has data
		/// </summary>
		public bool ExternalTableHasData
		{
			get
			{
				IExternalTableStore tableStore = GlobalDC.Container.Get<IMetadataManager>().MetadataStore.ExternalTableStore;
				return tableStore.StoreExists( Attributes.Id ) && GetRowCount() > 0;
			}
		}

		/// <summary>
		/// Adds/Modifies/Deletes row data in an external table
		/// </summary>
		/// <param name="rawData">Raw JSON data to add/modify/delete</param>
		/// <param name="opToPerform">Add/Delete operation being performed</param>
		/// <returns>Number of rows processed</returns>
		public int ProcessDataBlock( dynamic rawData, EExternalTableOperation opToPerform )
		{
			lock( m_DataProcessLock )
			{
				// Create return value
				int processedRowCount = 0;

				// Create a datatable matching the external mapping
				DataTable extDataTable = new DataTable();
				List<DataColumn> columnList = new List<DataColumn>();
				foreach( FieldMapping currCol in FieldMappings )
				{
					// If this is a remove then ignore all but key columns
					if( opToPerform == EExternalTableOperation.Delete && !currCol.SourceField.ColumnInfo.IsKey )
						continue;

					// Build column
					string columnName = currCol.TargetField.Name;
					Type dotNetType = DataTypeConvert.TranslateDataType( currCol.SourceField.ColumnInfo.DataType );
					DataColumn column = new DataColumn( columnName, dotNetType );
					column.AllowDBNull = currCol.SourceField.ColumnInfo.IsNullable && ( TableType != EExternalTableType.Analysis || !currCol.SourceField.ColumnInfo.IsKey );
					if( currCol.SourceField.ColumnInfo.DataType == EDataType.Text || currCol.SourceField.ColumnInfo.DataType == EDataType.Geographic )
						column.MaxLength = currCol.SourceField.ColumnInfo.Length;
					if( currCol.SourceField.ColumnInfo.DataType == EDataType.Guid )
						column.MaxLength = 36;
					columnList.Add( column );
					extDataTable.Columns.Add( column );
				}

				// If this is an add/update, add the auto identity column at the start and special control columns at the end
				if( opToPerform != EExternalTableOperation.Delete )
				{
					DataColumn autoIdCol = new DataColumn( "ExternalId", typeof( Int64 ) );
					autoIdCol.AllowDBNull = false;
					autoIdCol.AutoIncrement = true;
					autoIdCol.AutoIncrementSeed = -1;
					autoIdCol.AutoIncrementStep = -1;
					extDataTable.Columns.Add( autoIdCol );
					autoIdCol.SetOrdinal( 0 );

					DataColumn loadStateCol = new DataColumn( "ExtTabLoadState", typeof( Int32 ) );
					loadStateCol.AllowDBNull = false;
					extDataTable.Columns.Add( loadStateCol );

					DataColumn loadDateCol = new DataColumn( "ExtTabTimeStamp", typeof( DateTime ) );
					loadDateCol.AllowDBNull = false;
					extDataTable.Columns.Add( loadDateCol );
				}

				// If enumerable call repeatedly else call once
				DateTime startDateTime = DateTime.UtcNow;
				int? rowCount = rawData.Count;
				if( rowCount.HasValue )
				{
					foreach( dynamic currRow in rawData )
					{
						bool rowProcessed = ProcessDataRow( currRow, extDataTable, startDateTime, opToPerform );
						if( rowProcessed )
							processedRowCount++;
					}
				}
				else
				{
					bool rowProcessed = ProcessDataRow( rawData, extDataTable, startDateTime, opToPerform );
					if( rowProcessed )
						processedRowCount++;
				}

				// Now perform changes to the store
				IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
				IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;

				// Create the staging table
				tableStore.CreateStore( this );

				// Track added/deleted/updated records to notify associated objects when operation is completed
				List<long> addedRecords = new List<long>();
				List<long> deletedRecords = new List<long>();
				List<long> updatedRecords = new List<long>();

				// Delete operations are only permitted on external tables with enforced unique keys
				if( opToPerform == EExternalTableOperation.Delete && TableType == EExternalTableType.CampaignNonUniqueKeys )
					throw new InvalidOperationException( StringTables.Exceptions.RES_EXTERNALTABLE_CANNOTREMOVE_NONUNIQUE );
				else if( opToPerform == EExternalTableOperation.Delete )
					deletedRecords = tableStore.DeleteRows( this, extDataTable.CreateDataReader() );				
					
				// If external table allows non-unique keys simply append all records in the data block to the table
				// If unique keys are enforced, an upsert is required as the data block may contain both new and existing records
				if( opToPerform == EExternalTableOperation.Add && TableType == EExternalTableType.CampaignNonUniqueKeys )
					addedRecords = tableStore.AddRows( this, extDataTable.CreateDataReader(), startDateTime );
				else if( opToPerform == EExternalTableOperation.Add )
					addedRecords = tableStore.UpsertRows( this, extDataTable.CreateDataReader(), startDateTime, out updatedRecords );

				// Queue up task to notify associated objects of the latest operation
				Task notifyTask = new Task( () => RaiseObjectNotification( addedRecords, updatedRecords, deletedRecords ) );
				m_TaskQueue.QueueUpTask( notifyTask );

				// Return number of processed rows
				return processedRowCount;
			}
		}

		/// <summary>
		/// Gets the current row count for the external table's staging table
		/// </summary>
		/// <returns>Number of rows in the staging table</returns>
		public int GetRowCount()
		{
			IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
			IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;
			return tableStore.GetRowCount( this );
		}

		/// <summary>
		/// Get the store name for external table
		/// </summary>
		/// <returns>Working store name</returns>
		public string GetStoreName()
		{
			IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
			IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;
			return tableStore.GetStoreName( Attributes.Id );
		}

		/// <summary>
		/// Creates the external table store
		/// </summary>
		public void CreateStore()
		{
			IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
			IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;
			tableStore.CreateStore( this );
		}

		/// <summary>
		/// Get the campaign output fields for an external table
		/// </summary>
		/// <returns>List of fields to output</returns>
		public List<BasicDataSetColumnInfo> GetOutputFields()
		{
			// Create a field list containing all of the user-defined fields in the external table excluding any suppressed fields
			string storeName = GetStoreName();
			List<BasicDataSetColumnInfo> outputFields = new List<BasicDataSetColumnInfo>();
			foreach( FieldMapping fieldMapping in FieldMappings.Where( mapping => !mapping.SourceField.ColumnInfo.StateFlags.HasFlag( EStateFlags.Suppressed ) ) )
			{
				BasicDataSetColumnInfo colInfo = fieldMapping.SourceField.ColumnInfo.Clone() as BasicDataSetColumnInfo;
				colInfo.Name = fieldMapping.TargetField.Name;
				colInfo.TableName = storeName;
				outputFields.Add( colInfo );
			}

			return outputFields;
		}

		/// <summary>
		/// Auto update an external table which has an auto-update source
		/// </summary>
		/// <returns>Number of records loaded</returns>
		public int AutoUpdate()
		{
			// Create return value
			int retVal = 0;

			// Get the listener - TODO - log as a failure if listener not configured?
			IObjectAttributes currListener = null;			
			if( AutoUpdateSource != null && AutoUpdateSource.Id > 0 )
				currListener = ObjectManager.GetObjectAttributes( AutoUpdateSource.Id );
			if( currListener == null || currListener.IsDeleted || !currListener.IsValid || currListener.IsShortcut || currListener.IsTemplate || currListener.IsPlaceholder )
				return 0;
			ListenerEndpointResource listenerEndpointResource = ObjectManager.GetObject( currListener.Id ) as ListenerEndpointResource;
			if( listenerEndpointResource == null || String.IsNullOrWhiteSpace( listenerEndpointResource.ResourceLocation ) )
				return 0;

			// Log this
			GlobalDC.Container.Get<ILogger>().Log( StringTables.Messages.RES_AUTOUPDATINGEXTERNALTABLEFROMLISTENER( Attributes.Name, currListener.Name ) );

			// Split base address from full URL
			string baseAddress = listenerEndpointResource.ResourceLocation.Substring( 0, listenerEndpointResource.ResourceLocation.Trim().IndexOf( "/API/" ) );
			string extUrl = listenerEndpointResource.ResourceLocation.Substring( listenerEndpointResource.ResourceLocation.Trim().IndexOf( "/API/" ) );

			// Create an HTTPClient
			if( extUrl.StartsWith( "\\" ) )
				extUrl = extUrl.Substring( 1 );
			if( !extUrl.StartsWith( "/" ) )
				extUrl = "/" + extUrl;
			if( extUrl.EndsWith( "/" ) || extUrl.EndsWith( "\\" ) )
				extUrl = extUrl.Substring( 0, extUrl.Length - 1 );
			if( LastFetchedAutoUpdateEventId == null )
				LastFetchedAutoUpdateEventId = 0;
			extUrl += $"&firstEvent={LastFetchedAutoUpdateEventId + 1}&maxEventsPerCall=100";

			// Get an httpclient to do the work
			using( HttpClient clientToUse = GetHttpClient( baseAddress ) )
			{
				// Create async request
				Task<HttpResponseMessage> requestTask = clientToUse.GetAsync( extUrl );
				HttpResponseMessage responseMsg = requestTask.Result;
				Task<String> resultData = responseMsg.Content.ReadAsStringAsync();
				string listenerData = resultData.Result;

				// Read data
				dynamic rawData = JsonConvert.DeserializeObject( listenerData );
				retVal = ProcessDataBlock( rawData, EExternalTableOperation.Add );
			}

			// If we found some data upload it to analytic database now
			if( retVal > 0 )
				StartUploadData();

			// Return rows loaded
			return retVal;
		}

		#endregion // IExternalTable

		/// <summary>
		/// Gets the id of the external table
		/// </summary>
		public int TableId { get { return Table?.Id ?? 0; } }

		/// <summary>
		/// Processes a single row
		/// </summary>
		/// <param name="currRow">Row to process</param>
		/// <param name="extDataTable">Data table to store row in</param>
		/// <param name="startDateTime">Start time of this operation</param>
		/// <param name="opToPerform">Add/Delete operation being performed</param>
		/// <returns>Success flag</returns>
		private bool ProcessDataRow( dynamic currRow, DataTable extDataTable, DateTime startDateTime, EExternalTableOperation opToPerform )
		{
			// Check the row contains a full set of keys...
			//List<FieldMapping> keyCols = this.FieldMappings.Where( currMap => currMap.SourceField.ColumnInfo.IsKey ).
			//	OrderBy( currMap => currMap.SourceField.ColumnInfo.KeyOrdinal ).ToList();

			// Try to locate each field in the mapping in the dynamic and add it to the data table
			DataRow dataRow = extDataTable.NewRow();
			foreach( FieldMapping currCol in FieldMappings )
			{
				// If this is a remove then ignore all but key columns
				if( opToPerform == EExternalTableOperation.Delete && !currCol.SourceField.ColumnInfo.IsKey )
					continue;

				// Enumerate dot separated parts for sub-objects
				List<string> sourceColParts = currCol.SourceField.ColumnInfo.Name.Split( '.' ).ToList();
				object currColVal = currRow;
				while( sourceColParts.Count > 0 )
				{
					try
					{
						currColVal = ( (dynamic)( currColVal ) )[sourceColParts[0]];
					}
					catch
					{
						currColVal = null;
					}
					sourceColParts.RemoveAt( 0 );
					if( currColVal == null )
						break;
				}
				if( currColVal != null && ( (dynamic)( currColVal ) ).Value != null )
					dataRow[currCol.TargetField.Name] = ( (dynamic)( currColVal ) ).Value;
				else
					dataRow[currCol.TargetField.Name] = DBNull.Value;
			}

			// Add the basic details then add the row
			if( opToPerform != EExternalTableOperation.Delete )
			{
				dataRow["ExtTabLoadState"] = 1;
				dataRow["ExtTabTimeStamp"] = startDateTime;
			}
			extDataTable.Rows.Add( dataRow );

			// If the data contained an EventId and this is an autoupdate listener attached ET then update event id
			if( AutoUpdateSource != null && AutoUpdateSource.Id > 0 )
			{
				if( extDataTable.Columns.Contains( "EventId" ) )
				{
					int? eventId = Convert.ToInt32( dataRow["EventId"] ?? (int?)null );
					if( eventId.HasValue && eventId.Value > LastFetchedAutoUpdateEventId )
						LastFetchedAutoUpdateEventId = eventId.Value;
				}
			}

			// Processed successfully
			return true;
		}

		#region IObject

		/// <summary>
		/// Creates a copy of the current instance.
		/// </summary>
		/// <remarks>Reference members must be deep copied to ensure they do not 
		/// just contain a ref to the original object content</remarks>
		/// <returns>Cloned copy of this object</returns>
		public override object Clone()
		{
			// Base calls memberwise clone and clones its object properties
			ExternalTable clone = (ExternalTable)base.Clone();

			// Clone our properties and return
			clone.Table = (ObjectIdentity)Table?.Clone();
			return clone;
		}

		/// <summary>
		/// Gets the external table type id
		/// </summary>
		public override int GetTypeId()
		{
			return DataManagementObjectTypesAndGroups.ExternalTable;
		}

		/// <summary>
		/// Validates the object, returning a list of any validation failures if it is invalid
		/// </summary>
		/// <remarks>The Validity of an object can depend on the specific state of that
		/// object</remarks>
		/// <returns>Null if valid, list of validation failures if invalid</returns>
		public override List<ValidationError> Validate( ExecutableTaskDetails task = null )
		{
			// If we dont have a source name set to JSON for now
			if( String.IsNullOrWhiteSpace( SourceName ) )
				SourceName = "JSON";

			// Call base (external tables dont have an external resource currently)
			ValidateInternal( task, true, false );

			// Check the table - note this table doesnt necessarily exist yet in the ADS so may just have a name and no ID
			IObjectAttributes tableAttribs = null;
			if( ( Table?.Id ?? 0 ) != 0 )
			{
				try
				{
					Table.Name = ObjectManager.GetObjectAttributes( Table.Id ).Name;
				}
				catch( Exception )
				{
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ITEMNOTFOUNDWITHTYPENAME( "Table", $"Id: {Table.Id}" ), Source = "Table" } );
				}
			}
			if( ( ( Table?.Id ?? 0 ) == 0 && String.IsNullOrWhiteSpace( Table?.Name ) ) )
				ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_REQUIREDPROPERTYNOTDEFINED( "Table" ), Source = "Table" } );

			// If we have a table name now try to find and store the id
			if( !String.IsNullOrWhiteSpace( Table?.Name ) )
			{				
				tableAttribs = ObjectManager.FindObject( 0, Table.Name, 0, DataManagementObjectTypesAndGroups.Table, false );
				if( tableAttribs == null && TableType != EExternalTableType.Analysis )
				{
					// For campaigning tables the table must exist
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ITEMNOTFOUNDWITHTYPENAME( "Table", $"Name: {Table.Name}" ), Source = "Table" } );
				}
				else if( tableAttribs != null && IsNew && TableType == EExternalTableType.Analysis )
				{
					// For (new) analysis tables the table must not exist
					ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_TABLEALREADYEXISTS( Table.Name ), Source = "Table" } );
					Table.Id = 0;
					Table.Name = String.Empty;
				}
				else if( tableAttribs != null )
				{
					// If table was found, set the ID now
					Table.Id = tableAttribs.Id;
				}
			}

			// All external tables must have at least one key field defined
			if( !FieldMappings.Any( mapping => mapping.SourceField.ColumnInfo.IsKey ) )
				ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_NO_KEY_FIELDS_DEFINED, Source = "FieldMappings" } );

			// Validation block for campaign external table types i.e. triggered campaigns
			if( TableType == EExternalTableType.CampaignNonUniqueKeys || TableType == EExternalTableType.CampaignUniqueKeys )
			{
				// Validate that at least one field has been selected as include in output (if a field is not marked as 'included in output', the suppressed flag will be true)
				if( FieldMappings.All( x => x.SourceField.ColumnInfo.StateFlags.HasFlag( EStateFlags.Suppressed ) ) )
					ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_NO_FIELDS_INCLUDED_IN_OUTPUT, Source = "FieldMappings" } );

				// Cannot run an uploaddata task if table is used for campaigning
				if( task.Action == ExternalTableOperations.UploadData )
					ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_CANNOT_UPLOAD_CAMPAIGN_EXTERNAL_TABLE, Source = "TableType" } );

				// If saving/editing a campaign external table, check/ensure the base level is created by getting the base level info from the campaign manager
				if( tableAttribs != null && ( task.UserOperation == (int)EUserOperation.Save || task.UserOperation == (int)EUserOperation.Edit ) )
				{
					// Rebuild the base level if required but do not wait for it to rebuild here
					ICampaignManager campaignManager = GlobalDC.Container.Get<ICampaignManager>();
					BaseLevelInformation baseLevelInfo = campaignManager.GetBaseLevelInformation( Table, checkForRebuild: true, waitForRebuild: false );
					
					// Validate that all the required key fields for the base level are correctly defined in the field mappings
					baseLevelInfo.KeyColumns.ForEach( keyCol => 
					{
						FieldMapping keyColField = FieldMappings.FirstOrDefault( field => String.Equals( field.TargetField.Name, keyCol.Name, StringComparison.InvariantCultureIgnoreCase ) );
						if( keyColField == null || !keyColField.SourceField.ColumnInfo.IsKey )
							ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_PRIMARY_KEY_TARGET_FIELD_MISSING( keyCol.Name, tableAttribs.Name ), Source = "FieldMappings" } );
						else if( keyCol.DataType != keyColField.SourceField.ColumnInfo.DataType )
							ValidationErrors.Add( new ValidationError { Message = StringTables.Exceptions.RES_PRIMARY_KEY_TARGET_FIELD_INVALID_DATA_TYPE( keyColField.TargetField.Name ), Source = "FieldMappings" } );
					} );
				}
			}

			// Return any errors
			return ValidationErrors;
		}

		/// <summary>
		/// Indicates whether the current object has relevant diferences to the original
		/// </summary>
		/// <remarks>
		/// Performs a strategic comparison between this and the original object
		/// </remarks>
		/// <param name="original">The original object to comapre to</param>
		/// <returns>True if the current object is unchanged from the provided original</returns>
		public override bool UnChanged( object original )
		{
			ExternalTable originalExternalTable = original as ExternalTable;
			if( this == originalExternalTable )
				return true;
			if( originalExternalTable == null )
				return false;
			if( !base.UnChanged( originalExternalTable ) )
				return false;
			if( !IndigoHelpers.UnChanged( Table, originalExternalTable.Table ) )
				return false;
			else
				// No differences found
				return true;
		}

		/// <summary>
		/// Checks whether the object is dirty
		/// </summary>
		public override bool IsDirty()
		{
			return !UnChanged( ObjectManager.GetObject( Attributes.Id ) as IExternalTable );
		}

		/// <summary>
		/// Resets output only properties to their stored values
		/// </summary>
		/// <remarks>
		/// Some object properties are results generated by server processing. The client should not have 
		/// to store and return these when caching or updating an object, so this method applies the current 
		/// server stored property values to the current object.
		/// </remarks>
		/// <param name="storedObject">
		/// The stored or cached object containing the properties to update from
		/// </param>
		/// <param name="resetInstanceProperties">
		/// Should instance specific properties such as execution state be reset, 
		/// this would be required when saving an object as a new object
		/// </param>
		public override void ReApplyServerOnlyProperties( IObject storedObject, bool resetInstanceProperties )
		{
			base.ReApplyServerOnlyProperties( storedObject, resetInstanceProperties );

			// If no cached object, ensure that these properties are cleared - may be saving an existing 
			// object as a new one - if so output properties should be reset
			if( storedObject == null || resetInstanceProperties )
			{
				// Set defaults for outputonly properties
				// None currently
				return;
			}

			// Can only update these properties from an external table - sense check
			IExternalTable storedExternalTable = storedObject as IExternalTable;
			if( storedExternalTable == null )
				throw new ArgumentException( "Object supplied must be an IExternalTable", "storedObject" );

			// Reapply outputonly properties - use this weird assignment to clone - not sure if nullable types clone on assignment?
			LastFetchedAutoUpdateEventId = storedExternalTable.LastFetchedAutoUpdateEventId.HasValue ? storedExternalTable.LastFetchedAutoUpdateEventId.Value : (int?) null;
		}

		/// <summary>
		/// Save the object to a permanent store
		/// </summary>
		/// <returns>Null if object is unchanged by save, or the new object if saving
		/// changed the object (for example it saved as a different type)</returns>
		/// <param name="objAttribsToSave">Object attributes to include in the saved object
		/// state</param>
		/// <param name="performSecurityCheck">If cleared then check for user and creation in container etc rules
		/// are bypassed and object is created directly.</param>
		/// <param name="userModified">Flag whether this action was performed by a user</param>
		/// <param name="blockResRules">Optional block reservation rule to allow for reservation of 
		/// contiguous blocks of ids by name</param>
		public override IObject Save( IObjectAttributes objAttribsToSave, bool performSecurityCheck = true, bool userModified = true, BlockReservationRules blockResRules = null )
		{
			if( !IsNew && userModified )
			{
				// If the table already contains data we cannot change the table type
				IExternalTable currentDetails = ObjectManager.GetObject( Attributes.Id ) as IExternalTable;
				if( !UnChanged( currentDetails ) && GetRowCount() > 0 )
				{
					throw new InvalidOperationException( StringTables.Exceptions.RES_EXTERNALTABLE_CANNOT_CHANGE_DETAILS );
				}
				else if( !UnChanged( currentDetails ) )
				{
					// If the table is empty and the table type has changed, delete the store and it will be recreated when data is added
					IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
					IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;
					tableStore.DeleteStore( Attributes.Id );
				}
			}

			return base.Save( objAttribsToSave, performSecurityCheck, userModified, blockResRules );
		}

		#endregion // IObject


		/// <summary>
		/// Gets the list of task actions available on this type
		/// </summary>
		/// <remarks>
		/// Tasks may/not support scheduling by design. This interface provides/mantains a list of operations
		/// supported/available for execution and/or scheduling actions
		/// </remarks>
		[XmlIgnore]
		[JsonIgnore]
		public override List<ExecutableTaskDetails> PossibleTasks
		{
			get
			{
				// Return all possible tasks for seeds
				return new List<ExecutableTaskDetails> {
					{ new ExecutableTaskDetails { Action = ExternalTableOperations.UploadData, Description =  StringTables.Messages.RES_UPLOAD_DATA,
						Requirements = EExecutableTaskRequirements.FullLockOnStart | EExecutableTaskRequirements.SaveOnStart ,
						IsSchedulable = true, AlwaysSchedule = false, RaiseNotifications = EUserNotificationCategory.DataManager,
						ExecuteTask =  ( TaskParameters taskParams ) => CreateTaskWrapper( StringTables.Messages.RES_UPLOADINGDATA( Attributes.Name, Table.Name ), taskParams, StartUploadData ) } } };
			}
		}

		/// <summary>
		/// Start asynchronously uploading data into the metadata store
		/// </summary>
		/// <remarks>
		/// Pushes changes from the holding database into the store
		/// </remarks>
		private void StartUploadData()
		{
			DateTime startTime = DateTime.UtcNow;

			try
			{
				// Do the work...
				using( ExtendedDbConnection dbConn = DbFactoryHelper.GetAnalyticConnection( GlobalDC.Container.Get<ISecurityHelper>().GetActiveWindowsIdentity() ) )
				{
					// Does the table exist?
					DataAccessFactory agnosticFactory = new DataAccessFactory();
					bool tableCreated = false;
					if( !dbConn.CheckTableExists( null, Table.Name ) )
					{
						// Build create table command
						StringBuilder createCmd = new StringBuilder( 1024 );
						createCmd.Append( $"CREATE TABLE [{Table.Name}] ( " );

						// Build column definitions
						List<String> colDefs = new List<string>();
						foreach( FieldMapping colMap in FieldMappings )
							colDefs.Add( $"{agnosticFactory.EncloseDbObject( colMap.TargetField.Name )} {agnosticFactory.CreateColumnDefintion( colMap.SourceField.ColumnInfo, true )}" );

						// Build primary keys constraint if present
						IEnumerable<string> primaryKeyCols = FieldMappings.Where( currCol => currCol.SourceField.ColumnInfo.IsKey ).
							OrderBy( currCol => currCol.SourceField.ColumnInfo.KeyOrdinal ).
							Select( currCol => agnosticFactory.EncloseDbObject( currCol.TargetField.Name ) );
						if( primaryKeyCols.Count() > 0 )
						{
							string primaryKeyList = String.Join( ", ", primaryKeyCols );
							colDefs.Add( $"PRIMARY KEY ( {primaryKeyList} )" );
						}
						string colDefsString = String.Join( ", ", colDefs );
						createCmd.Append( colDefsString );
						createCmd.Append( " )" );

						// Create the table
						using( IDbCommand command = dbConn.CreateCommand() )
						{
							command.CommandText = createCmd.ToString();
							command.ExecuteNonQuery();
						}
						tableCreated = true;
					}
					else
					{
						// This really should be done for all DBs but ADS is weird in that the insert reinserts the full table (as it is the source for the upload)
						// So, a bit hacky but dont do the clear if this is an ADS
						if( dbConn.DataFactory.Name != "ADS Server" )
						{
							String clearCommand = dbConn.DataFactory.ClearTable;
							if( String.IsNullOrWhiteSpace( clearCommand ) )
								clearCommand = $"DELETE FROM [@TableName]";
							clearCommand = clearCommand.Replace( "@TableName", Table.Name );
							using( IDbCommand command = dbConn.CreateCommand() )
							{
								command.CommandText = clearCommand;
								command.ExecuteNonQuery();
							}
						}
					}


					// Get the external table store name
					IMetadataManager metaManager = GlobalDC.Container.Get<IMetadataManager>();
					IExternalTableStore tableStore = metaManager.MetadataStore.ExternalTableStore;
					string sourceTableName = tableStore.GetStoreName( Attributes.Id );

					// Get metadata connection details
					string metaConnStr = ( (IndigoAppSettings)( GlobalDC.Container.Get<IAppSettings>() ) ).ConnectionString;
					string metaDbName = ( (IndigoAppSettings)( GlobalDC.Container.Get<IAppSettings>() ) ).FullApplicationName;

					// Build column list
					List<String> colNames = new List<string>();
					foreach( FieldMapping colMap in FieldMappings )
						colNames.Add( $"{agnosticFactory.EncloseDbObject( colMap.TargetField.Name )}" );
					string colList = String.Join( ", ", colNames );

					// Get metadata factory
					string metaFactoryName = GlobalDC.Container.Get<IDataAccessSettings>().RepoSettings.First( x => x.Key == "DbFactoryName" ).Value;

					// Build the insert cross database command
					StringBuilder insertCmd = new StringBuilder( 1024 );
					insertCmd.Append( $"INSERT INTO [{Table.Name}] SELECT {colList} FROM [EXTERNALDBPROVIDER:OLEDB:CONNSTR:Provider=SQLOLEDB.1;{metaConnStr}:DATABASE:{metaDbName}:TABLE:{sourceTableName}:FACTORY:{metaFactoryName}] where [ExtTabLoadState] != {(int)EExternalTableLoadState.Uploaded}" );

					// Populate the table using the cross db insert syntax
					using( IDbCommand command = dbConn.CreateCommand() )
					{
						command.CommandText = insertCmd.ToString();
						command.ExecuteNonQuery();
					}

					// If we created this table this time....
					if( tableCreated )
					{
						// Synchronise just the one change (new table) into the metadata store
						try
						{
							FilterCollection omFilters = new FilterCollection();
							omFilters.Add( new ObjectManagementFilter { ObjectType = DataManagementObjectTypesAndGroups.Database } );
							IObjectAttributes parentDb = ObjectManager.List( omFilters ).FirstOrDefault();
							if( parentDb != null )
							{
								// If we find any diffs commit them
								DatabaseCompare dbCompare = new DatabaseCompare();
								dbCompare.Rebuild( parentDb.Id, true, Table.Name );
								if( dbCompare.TotalDiffs > 0 )
									dbCompare.ApplyChanges();
							}
						}
						catch( Exception ex )
						{
							// Non critical error - log but dont throw - can resolve this diff manually
							GlobalDC.Container.Get<ILogger>().Log( "ExternalTable:StartUploadData", ex, EErrorTraceLevel.Error );
						}
					}
				}

				// Set last run date
				LastRunDate = DateTime.UtcNow;
				SaveWhileExecuting( false );
			}
			catch( Exception ex )
			{
				m_Progress.ProgressDetails = StringTables.Messages.RES_UPLOAD_DATA_FAILED( ex.Message );
				m_Progress.ProgressState = EProgressState.Stopped | EProgressState.Failed;
				m_Progress.ProgressFailureException = ex;
				m_Progress.DoneProgress();
				throw;
			}
			finally
			{
				// Log the task run detail if we got an exception
				if( m_Progress.ProgressFailureException != null )
				{
					ExecutionInfo taskRunItemDetails = new ExecutionInfo( m_Progress.ProgressFailureException );
					TaskRunDetail taskRunDetail = new TaskRunDetail();
					taskRunDetail.TaskRunId = TaskRunId;
					taskRunDetail.SubItemId = -1;
					taskRunDetail.Details = taskRunItemDetails;
					taskRunDetail.Status = (int)m_Progress.ProgressState;
					taskRunDetail.StartTime = startTime;
					taskRunDetail.EndTime = DateTime.UtcNow;

					GlobalDC.Container.Get<IServerManager>().GetActivityLogStore().CreateTaskRunDetail( TaskRunId, taskRunDetail );
				}
			}
		}

		/// <summary>
		/// Raise an object notification with details of added/updated/removed records
		/// </summary>
		/// <param name="addedRecords">Collection of records added</param>
		/// <param name="updatedRecords">Collection of records updated</param>
		/// <param name="deletedRecords">Collection of records removed</param>
		private void RaiseObjectNotification( List<long> addedRecords, List<long> updatedRecords, List<long> deletedRecords )
		{
			// TODO: If there is a post processor defined, call it before raising the notification

			// Set up event parameters
			AssociativeArray eventParameters = new AssociativeArray();
			eventParameters.SetValue( EExternalTableOperation.Add.ToString(), addedRecords );
			eventParameters.SetValue( EExternalTableOperation.Update.ToString(), updatedRecords );
			eventParameters.SetValue( EExternalTableOperation.Delete.ToString(), deletedRecords );

			// Raise the notification
			ObjectManager.SendNotification( EObjectNotificationEventTypes.ObjectAction, Attributes, eventParameters );
		}

		/// <summary>
		/// Gets a new http client
		/// </summary>
		/// <param name="baseAddress">Address this client is to operate on</param>
		private HttpClient GetHttpClient( string baseAddress )
		{
			// Create http client
			HttpClientHandler noAutoRedirectHandler = new HttpClientHandler { AllowAutoRedirect = false };
			HttpClient retHttpClient = new HttpClient( noAutoRedirectHandler );
			retHttpClient.BaseAddress = new Uri( baseAddress );

			// Set timeout on requests
			retHttpClient.Timeout = new TimeSpan( 0, 0, 30 );

			// Set accept header to JSON
			retHttpClient.DefaultRequestHeaders.Accept.Clear();
			retHttpClient.DefaultRequestHeaders.Accept.Add( new MediaTypeWithQualityHeaderValue( "application/json" ) );

			// Return generated httpclient
			return retHttpClient;
		}

	}
}
