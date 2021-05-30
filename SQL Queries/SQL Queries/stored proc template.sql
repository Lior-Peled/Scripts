/****** Object:  StoredProcedure [dbo].[USP_RI_STG_DimActivity_Fact]    Script Date: 8/11/2020 10:32:52 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE   
AS                  
BEGIN
       
	
		
SET NOCOUNT ON
begin try

Begin transaction

--code here

commit transaction;

end try

  begin catch

  if @@TRANCOUNT>0    rollback transaction;

  throw

  end catch

  END




