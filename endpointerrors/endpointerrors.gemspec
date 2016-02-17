Gem::Specification.new do |s|
  s.name        = 'endpointerrors'
  s.version     = '1.0'
  s.date        = '2016-02-17'
  s.summary     = "Simple capture and logging system for errors reported from endpoint via SQS"
  s.description = "Endpoint Error Capture system"
  s.authors     = ["Andy Gallagher"]
  s.email       = 'andy.gallagher@theguardian.com'
  s.executables = ["trap_endpoint_errors.rb"]
  s.homepage    =
    ''
  s.license       = 'GNM'
  s.add_dependency(%q<json>)
  s.add_dependency(%q<awesome_print>)
  s.add_dependency(%q<aws-sdk-core>)
  s.add_dependency(%q<aws-sdk-resources>)
  
end
