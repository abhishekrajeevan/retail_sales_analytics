create or replace function processing_catalog.schema_harmonized_dimension.masking_phone(phone_number string)
return 
  case 
    when is_member("CRM") then phone_number
    else  "**********"
  end;

create or replace function processing_catalog.schema_harmonized_dimension.masking_email(email string)
return 
  case 
    when is_member("CRM") then email
    else  concat('***', substr(email, instr(email, '@'))) 
  end;

create or replace function processing_catalog.schema_harmonized_dimension.masking_address(address string)
return 
  case 
    when is_member("CRM") then address
    else  element_at(split(address, ","), -1)
  end;
