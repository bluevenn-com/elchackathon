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
//	28/09/2020	E.Hall	    -			First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using BlueVenn.Indigo.Core.CSV;
using BlueVenn.Indigo.DataAccess.Shared;
using BlueVenn.Indigo.DataManagement.Shared.Interfaces;

namespace BlueVenn.Indigo.DataManagement.Server.Objects
{
	/// <summary>
	/// ListenerEndpointResource class
	/// </summary>
	/// <remarks>This is an implementation of the ExternalResource class in which the resource location is a listener endpoint.</remarks>
	public class ListenerEndpointResource : ExternalResource
	{ 
		/// <summary>
		/// Gets the type id that implements this object
		/// </summary>
		/// <remarks>
		/// All objects are implemented with a unique type id
		/// </remarks>
		public override int GetTypeId()
		{
			return DataManagementObjectTypesAndGroups.ListenerEndpointResource;
		}

		/// <summary>
		/// The resource location
		/// </summary>
		public string ResourceLocation { get; set; }

		/// <summary>
		/// Constructor
		/// </summary>
		public ListenerEndpointResource()
		{

		}

		/// <summary>
		/// Fetch data from the resource location
		/// </summary>
		/// <param name="commandText">SQl syntax to fetch the data</param>
		/// <param name="dataProcessor">Delegate function to process the datareader</param>
		public override void GetResourceData( string commandText, Action<IDataReader> dataProcessor )
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Fetch data from the resource location
		/// </summary>
		/// <param name="fieldMappings">Details of fields to extract from external resource, or null to return all fields (ie select *)</param>
		/// <param name="sourceName">Name of external resource source (e.g. table or file name)</param>
		/// <param name="dataProcessor">Delegate function to process the datareader</param>
		public override void GetResourceData( IEnumerable<FieldMapping> fieldMappings, string sourceName, Action<IDataReader> dataProcessor )
		{
			GetResourceDataInternal( fieldMappings, sourceName, dataProcessor, 0 );
		}

		/// <summary>
		/// Fetch data from the resource location
		/// </summary>
		/// <param name="dataReader">The datareader</param>
		/// <param name="dataProcessor">Delegate function to process the datareader</param>
		private void GetResourceData( IDataReader dataReader, Action<IDataReader> dataProcessor )
		{
			using( dataReader )
			{
				dataProcessor( dataReader );
			}
		}

		/// <summary>
		/// Fetch a sample (20 rows) of data from the resource location
		/// </summary>
		/// <param name="commandText">SQl syntax to fetch the data</param>
		/// <returns>Data table with sample data</returns>
		public override DataTable GetResourceSample( string commandText )
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Fetch a sample of data from the resource location
		/// </summary>
		/// <param name="fieldMappings">Details of fields to extract from external resource or null for all fields (ie select *)</param>
		/// <param name="sourceName">Name of external resource source (e.g. table or file name)</param>
		/// <returns>Data table with sample data</returns>
		public override DataTable GetResourceSample( IEnumerable<FieldMapping> fieldMappings, string sourceName )
		{
			DataTable sample = null;
			GetResourceDataInternal( fieldMappings, sourceName, ( IDataReader reader ) =>
			{
				sample = SampleDataReader( reader, SampleSize );
			}, SampleSize );
			return sample;
		}

		/// <summary>
		/// Fetches names of sources for this external resource object
		/// </summary>
		/// <returns>
		/// Sources will vary depending on the external resource type e.g. tables and views for database resources
		/// or filenames for filesystem resources
		/// </returns>
		public override IEnumerable<string> GetSourceNames()
		{
			List<string> tableNames = new List<string>();
			tableNames.Add( Path.GetFileName( ResourceLocation ) );
			return tableNames;
		}

		/// <summary>
		/// Fetch data from the resource location
		/// </summary>
		/// <param name="fieldMappings">Details of fields to extract from external resource, or null to return all fields (ie select *)</param>
		/// <param name="sourceName">Name of external resource source (e.g. table or file name)</param>
		/// <param name="dataProcessor">Delegate function to process the datareader</param>
		/// <param name="rowLimit">Attempt to apply limit for rows in sql query (may not be supported on some providers)</param>
		private void GetResourceDataInternal( IEnumerable<FieldMapping> fieldMappings, string sourceName, Action<IDataReader> dataProcessor, int rowLimit )
		{
			if( rowLimit > 0 )
			{
				if( rowLimit > MaxRowLimit )
					throw new ArgumentOutOfRangeException( nameof( rowLimit ), $"Max row limit is {MaxRowLimit}" );
			}

			FlatFileReader rawReader = new FlatFileReader( ResourceLocation );
			if( fieldMappings != null )
			{
				AdaptedDataReader<FlatFileReader, DbConnection> adaptedReader = new AdaptedDataReader<FlatFileReader, DbConnection>( rawReader );
				adaptedReader.MaxRows = rowLimit;
				AdaptedDataColumnList rebindList = new AdaptedDataColumnList();
				int currPos = 0;
				foreach( FieldMapping currMap in fieldMappings )
				{
					int origPos = rawReader.GetOrdinal( currMap.SourceField.ColumnInfo.Name );
					rebindList.AddBinding( currPos, origPos, rawReader.GetFieldType( origPos ), rawReader.GetName( origPos ) );
					currPos++;
				}
				adaptedReader.SetColumnRebinding( rebindList );

				GetResourceData( adaptedReader, dataProcessor );
			}
			else
			{
				GetResourceData( rawReader, dataProcessor );
			}
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
			ListenerEndpointResource originalResource = original as ListenerEndpointResource;
			if( !base.UnChanged( originalResource ) )
			{
				return false;
			}
			else if( this == originalResource )
			{
				return true;
			}

			// No differences found
			return true;
		}
	}
}