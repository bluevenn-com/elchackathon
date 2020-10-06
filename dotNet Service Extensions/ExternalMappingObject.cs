//-----------------------------------------------------------------------------
// © 2020 BlueVenn Ltd
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.DataManagement.Shared
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
using System.Xml.Serialization;
using BlueVenn.Indigo.Core;
using BlueVenn.Indigo.Core.Async;
using BlueVenn.Indigo.Core.Resources;
using BlueVenn.Indigo.DataAccess.Shared;
using BlueVenn.Indigo.ObjectManagement.Shared.Interfaces;
using BlueVenn.Indigo.ServerManagement.Shared.Interfaces;
using Newtonsoft.Json;

namespace BlueVenn.Indigo.DataManagement.Shared.Interfaces
{
	/// <summary>
	/// Base class for all objects which map data
	/// </summary>
	/// <remarks>
	/// Mapping objects define the mapping rules between an external resource and an internal
	/// storage. Examples include external seeds, external data, dynamic data etc
	/// </remarks>
	public abstract class ExternalMappingObject : ExecutableTaskObject, IExternalMappingObject
	{
		/// <summary>
		/// The source entity name
		/// </summary>
		/// <remarks>
		/// Name of source item understood by the mapping itself.
		/// For example this might be a table or view in an external data/seed object
		/// </remarks>
		public string SourceName { get; set; }

		/// <summary>
		/// The external resource object from which to fetch data
		/// </summary>
		/// <remarks>
		/// May be null. Not all mappings are built against an external resource
		/// </remarks>
		public ObjectIdentity ExternalResource { get; set; }

		/// <summary>
		/// Get or set the last date and time this mapping object was executed
		/// </summary>
		public DateTime? LastRunDate { get; set; }

		/// <summary>
		/// Get or set the expiry date of the external mapping object
		/// </summary>
		public WaitCondition ExpiryCondition { get; set; }

		/// <summary>
		/// Gets or sets field mappings
		/// </summary>
		public List<FieldMapping> FieldMappings { get; set; }

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
			ExternalMappingObject clone = (ExternalMappingObject)base.Clone();

			// Now clone my properties and return
			clone.ExternalResource = (ObjectIdentity)ExternalResource?.Clone();
			clone.ExpiryCondition = (WaitCondition)ExpiryCondition?.Clone();
			clone.FieldMappings = FieldMappings?.Select( m => (FieldMapping)m?.Clone() ).ToList();
			return clone;
		}

		/// <summary>
		/// Validates the object, returning a list of any validation failures if it is invalid
		/// </summary>
		/// <remarks>The Validity of an object can depend on the specific state of that
		/// object</remarks>
		/// <param name="task">Optional task to validate against. Some validation might be specific to the task being performed.
		/// This will be null for normal validation on save etc.</param>
		/// <returns>Null if valid, list of validation failures if invalid</returns>
		public override List<ValidationError> Validate( ExecutableTaskDetails task = null )
		{
			// By default our validation checks mappings are set up fully
			return ValidateInternal( task, true );
		}

		/// <summary>
		/// Validates the object, returning a list of any validation failures if it is invalid
		/// </summary>
		/// <remarks>The Validity of an object can depend on the specific state of that
		/// object</remarks>
		/// <param name="task">Optional task to validate against. Some validation might be specific to the task being performed.
		/// This will be null for normal validation on save etc.</param>
		/// <param name="checkSourceAndMappings">If set the source name and mappings are validated</param>
		/// <param name="checkExternalResource">If set the external resource is validated</param>
		/// <returns>Null if valid, list of validation failures if invalid</returns>
		protected List<ValidationError> ValidateInternal( ExecutableTaskDetails task, bool checkSourceAndMappings = true, bool checkExternalResource = true )
		{
			// We don't want to call base validate as Table property will be null for seeds
			base.Validate( task );

			// Check external resource
			if( checkExternalResource )
			{
				if( ExternalResource == null || ExternalResource.Id == 0 )
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ARGNULLOREMPTY( nameof( ExternalResource ) ), Source = nameof( ExternalResource ) } );
				else if( !ObjectManager.FindObject( ExternalResource.Id ) )
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ITEMNOTFOUNDWITHTYPENAME( ObjectManager.ObjectTypes[DataManagementObjectTypesAndGroups.ExternalResource].Name, ExternalResource.Name ), Source = nameof( ExternalResource ) } );
				else if( checkSourceAndMappings && !String.IsNullOrWhiteSpace( SourceName ) )
				{
					// Source name is concatenated into sql query... make sure it is actually one of the source names not some sql injection attack!
					IExternalResource externalResource = ObjectManager.GetObject( ExternalResource.Id ) as IExternalResource;
					IEnumerable<string> sourceNames = externalResource.GetSourceNames();
					if( !sourceNames.Any( source => source.Equals( SourceName, StringComparison.Ordinal ) ) )
						ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ITEMNOTFOUND( SourceName ), Source = nameof( SourceName ) } );
				}
			}

			// If we are validatiing source mappings...
			if( checkSourceAndMappings )
			{
				// Check source name
				if( String.IsNullOrWhiteSpace( SourceName ) )
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ARGNULLOREMPTY( nameof( SourceName ) ), Source = nameof( SourceName ) } );

				// Check field mappings
				if( FieldMappings == null || FieldMappings.Count < 1 )
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ARGNULLOREMPTY( nameof( FieldMappings ) ), Source = nameof( FieldMappings ) } );
				else if( FieldMappings.Any( m => m.SourceField == null || m.SourceField.ColumnInfo == null) )
					ValidationErrors.Add( new ValidationError { Message = SharedStringTables.Shared.RES_ARGUMENTISINVALID( nameof( FieldMappings ) ), Source = nameof( FieldMappings ) } );
			}

			// Convert all datetimes to UTC prior to save
			if( LastRunDate.HasValue )
				LastRunDate = LastRunDate.Value.ToUniversalTime();

			// Return validation errors
			return ValidationErrors;
		}

		/// <summary>
		/// Returns true if this resource has unsaved changes
		/// </summary>
		/// <remarks>
		/// This performs a stategic comparison to an original object.
		/// This will ignore any usage and other system controlled changes.
		/// </remarks>
		/// <returns>
		/// True if this resource has unsaved changes
		/// </returns>
		public override bool IsDirty()
		{
			return !UnChanged( ObjectManager.GetObject( Attributes.Id ) as ExternalMappingObject );
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
			ExternalMappingObject originalMapObject = original as ExternalMappingObject;
			if( this == originalMapObject )
				return true;
			else if( !base.UnChanged( originalMapObject ) )
				return false;
			else if( !( StringComparer.Ordinal.Compare( SourceName, originalMapObject.SourceName ) == 0 ) )
				return false;
			else if( !IndigoHelpers.TestAll( FieldMappings, originalMapObject.FieldMappings, IndigoHelpers.UnChanged ) )
				return false;
			else if( !IndigoHelpers.UnChanged( ExternalResource, originalMapObject.ExternalResource ) )
				return false;
			else if( !IndigoHelpers.UnChanged( ExpiryCondition, originalMapObject.ExpiryCondition ) )
				return false;
			else
				// No differences found
				return true;
		}

		/// <summary>
		/// Checks whether the object is undefined
		/// </summary>
		public override bool IsUndefined()
		{
			// Mapping objects are regarded as undefined if they have no external resource
			return ExternalResource == null;
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
			// If no cached object, ensure that these properties are cleared - may be saving an existing 
			// object as a new one - if so output properties should be reset
			if( storedObject == null || resetInstanceProperties )
			{
				// Set defaults for outputonly properties
				LastRunDate = null;
				return;
			}

			// Can only update these properties from a model - sense check
			ExternalMappingObject storedExternalMap = storedObject as ExternalMappingObject;
			if( storedExternalMap == null )
				throw new ArgumentException( "Object supplied must be an ExternalMappingObject", "storedObject" );

			// Reapply outputonly properties
			LastRunDate = storedExternalMap.LastRunDate;

			// Call base
			base.ReApplyServerOnlyProperties( storedObject, resetInstanceProperties );
		}

		#endregion // IObject

		#region IExecutableTask

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
				return new List<ExecutableTaskDetails> {
					{ new ExecutableTaskDetails { Action = ExternalDataOperations.FetchData, Description = SharedStringTables.Shared.RES_FETCH_DATA,
						Requirements = EExecutableTaskRequirements.FullLockOnStart | EExecutableTaskRequirements.SaveOnStart ,
						IsSchedulable = true, AlwaysSchedule = true, RaiseNotifications = EUserNotificationCategory.DataManager,
						ExecuteTask =  ( TaskParameters taskParams ) => CreateTaskWrapper( SharedStringTables.Shared.RES_FETCHINGEXTERNALDATA( Attributes.Name, ExternalResource.Name ), taskParams, StartFetchData ) } } };
			}
		}

		#endregion

		/// <summary>
		/// Start asynchronously fetching external data
		/// </summary>
		protected virtual void StartFetchData()
		{
			try
			{
				FetchDataInternal();
			}
			catch( Exception ex )
			{
				m_Progress.ProgressDetails = SharedStringTables.Shared.RES_FETCH_EXTERNAL_DATA_FAILED( ex.Message );
				m_Progress.ProgressState = EProgressState.Stopped | EProgressState.Failed;
				m_Progress.ProgressFailureException = ex;
				m_Progress.DoneProgress();
				throw;
			}
		}

		/// <summary>
		/// Fetch the external data into the metadata store
		/// </summary>
		/// <remarks>
		/// Fetches data into the store and then saves the object. Saving the object will fail if the object is currently locked with a write lock
		/// </remarks>
		public virtual bool RefreshIfRequired()
		{
			bool needsRefresh = false;

			if( !LastRunDate.HasValue )
				needsRefresh = true;
			else
			{
				if( ExpiryCondition != null && ExpiryCondition.WaitType == EWaitType.WaitForInterval )
				{
					DateTime expiryDate = LastRunDate.Value
					.AddDays( ExpiryCondition.WaitForInterval.Days )
					.AddHours( ExpiryCondition.WaitForInterval.Hours )
					.AddMinutes( ExpiryCondition.WaitForInterval.Minutes );

					needsRefresh = ( expiryDate < DateTime.UtcNow );
				}
				else if( ExpiryCondition != null && ExpiryCondition.WaitType == EWaitType.WaitUntilDateTime )
				{
					needsRefresh = ( ExpiryCondition.WaitUntilDateTime < DateTime.UtcNow );
				}
			}

			if( !needsRefresh )
				return false;

			FetchDataInternal();
			ObjectManager.UpdateObjectDetails( this, Attributes, "ExternalMappingObject.FetchData" );

			return true;
		}

		/// <summary>
		/// Fetch the external data into the metadata store
		/// </summary>
		protected virtual void FetchDataInternal()
		{
			// Base impl does nothing
		}
	}
}
