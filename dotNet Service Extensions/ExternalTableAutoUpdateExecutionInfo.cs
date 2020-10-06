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
//	02/10/2020	J.Boyce 				First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------

using System.Collections.Generic;
using BlueVenn.Indigo.DataManagement.Shared.Resources;
using BlueVenn.Indigo.ServerManagement.Shared.Interfaces;
using PostSharp.Patterns.Diagnostics;

namespace BlueVenn.Indigo.DataManagement.Shared.Interfaces
{
	/// <summary>
	/// Holds task execution results info for external table auto update
	/// </summary>
	public class ExternalTableAutoUpdateExecutionInfo : ExecutionInfo
	{
		/// <summary>
		/// Gets or sets the external table being auto uploaded
		/// </summary>
		public string ExternalTableName { get; set; }

		/// <summary>
		/// Gets or sets the external resource name being used to auto upload the external table
		/// </summary>
		public string ExternalResourceName { get; set; }

		/// <summary>
		/// Gets or sets the number of records loaded
		/// </summary>
		public int NumRecords { get; set; }

		/// <summary>
		/// Build the dynamically generated execution info message
		/// </summary>
		/// <returns>List of localised messages describing the execution info</returns>
		[Log( AttributeExclude = true )]
		protected override List<string> CreateMessage()
		{
			// Build and return message
			List<string> messages = new List<string>();
			messages.Add( StringTables.Messages.RES_AUTOUPDATEDEXTERNALTABLEFROMLISTENER( ExternalTableName, ExternalResourceName, NumRecords ) );
			return messages;
		}
	}
}
