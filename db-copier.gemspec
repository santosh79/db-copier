# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{db-copier}
  s.version = "0.0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Santosh Kumar"]
  s.date = %q{2010-01-18}
  s.email = %q{santosh79@gmail.com}
  s.files = [
    "lib/db-copier.rb",
    "lib/db-copier/db-copier.rb",
    "lib/db-copier/worker.rb",
    "README.txt",
    "spec/spec_helper.rb",
    "spec/basic_spec.rb"]
  s.has_rdoc = false
  s.homepage = %q{http://github.com/santosh79/db-copier}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.5}
  s.summary = %q{A DSL around Sequel to aid in copying or replicating databases.}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
