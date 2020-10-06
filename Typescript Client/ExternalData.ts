//-----------------------------------------------------------------------------
// (c) 2020 BlueVenn ltd
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.UI.DataManagement
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	03/10/2020	E.Hall 	-			First release
//
//-----------------------------------------------------------------------------
// Dependencies/Imports/Exports
// ----------------------------

import "text!./ExternalData.html";

import ifwk = Indigo.Framework;

export var template: string = require("text!./ExternalData.html");

export class viewModel
{
    public viewModel: ifwk.ViewModels.IObjectViewModel;

    /**
     * Constructor
     * @param params
     */
    constructor(params: any)
    {
        this.viewModel = params.viewModel;
    }
}
