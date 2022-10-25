# Copyright (c) .NET Foundation and contributors. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

# This is designed to be a stand-alone, out-of-process kernel, primarily used to ensure proper proxy
# handling.  It requires a local `perl.exe` interpreter be available and can be used by executing
# the following in a notebook:
#
#     #!connect stdio --kernel-name perl --command perl.exe kernel.pl

use JSON;
use Try::Tiny;

$|++; # autoflush

%suppressedValues = {};
foreach my $valueName ( keys %main:: ) {
    if (!$suppressedValues{$valueName}) {
        $suppressedValues{$valueName} = 1;
    }
}

my $kernelUri = "kernel://perl-$$";

$kernelInfo = {
    "localName" => "perl",
    "languageName" => "perl",
    "languageVersion" => "$^V",
    "displayName" => "Perl $^V",
    "uri" => $kernelUri,
    "supportedKernelCommands" => [
        { "name" => "RequestKernelInfo" },
        { "name" => "RequestValue" },
        { "name" => "RequestValueInfos" },
        { "name" => "SendValue" },
        { "name" => "SubmitCode" }
    ],
    "supportedDirectives" => []
};

# run with `perl.exe kernel.pl test` to ensure the value serialization works as expected
if (length(@ARGV) > 0 && $ARGV[0] eq "test") {
    $scalar = 4;
    @array = (1, 2, 3);
    $arrayRef = [4, 5, 6];
    %hash = ("theAnswer" => 42, "pi" => 3.14159);
    $hashRef = {"theAnswer" => 42, "pi" => 3.14159};
    ##
    @names = ("scalar", "array", "arrayRef", "hash", "hashRef");
    @mimeTypes = ("text/plain", "application/json");
    foreach my $mimeType (@mimeTypes) {
        print "$mimeType:\n";
        foreach my $name (@names) {
            print "    $name: " . getStringRepresentationOfValueName($name, $mimeType) . "\n";
        }
    }

    exit(0);
}

# otherwise this is the kernel's entry point
publish({
    "eventType" => "KernelReady",
    "event" => {},
    "command" => undef,
    "routingSlip" => [
        $kernelUri
    ]
});

publish({
    "eventType" => "KernelInfoProduced",
    "event" => {
        "kernelInfo" => $kernelInfo,
    },
    "command" => undef,
    "routingSlip" => [
        $kernelUri
    ]
});

while (<STDIN>) {
    chomp;
    try {
        $envelope = decode_json($_);
        $commandType = $envelope->{'commandType'};
        if ($commandType) {
            $token = $envelope->{'token'};
            $command = $envelope->{'command'};
            $succeeded = false;
            if ($commandType eq "RequestKernelInfo") {
                #
                #                                             RequestKernelInfo
                #
                publish({
                    "eventType" => "KernelInfoProduced",
                    "event" => {
                        "kernelInfo" => $kernelInfo
                    },
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
                $succeeded = true;
            } elsif ($commandType eq "RequestValue") {
                #
                #                                                  RequestValue
                #
                $valueName = $command->{'name'};
                $mimeType = $command->{'mimeType'};
                $formattedValue = "TODO";
                $formattedValue = getStringRepresentationOfValueName($valueName, $mimeType);
                publish({
                    "eventType" => "ValueProduced",
                    "event" => {
                        "name" => $valueName,
                        "formattedValue" => {
                            "mimeType" => $mimeType,
                            "value" => $formattedValue
                        }
                    },
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
                $succeeded = true;
            } elsif ($commandType eq "RequestValueInfos") {
                #
                #                                             RequestValueInfos
                #
                my @valueInfos = ();
                foreach my $valueName ( keys %main:: ) {
                    if (!$suppressedValues{$valueName}) {
                        push(@valueInfos, { "name" => "$valueName" });
                    }
                }
                publish({
                    "eventType" => "ValueInfosProduced",
                    "event" => {
                        "valueInfos" => \@valueInfos
                    },
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
                $succeeded = true;
            } elsif ($commandType eq "SendValue") {
                #
                #                                                     SendValue
                #
                $formattedValue = $command->{'formattedValue'};
                if ($formattedValue->{'mimeType'} eq "application/json") {
                    $valueName = $command->{'name'};
                    $jsonValue = $formattedValue->{'value'};
                    $runtimeValue = decode_json($jsonValue);
                    $main::{$valueName} = $runtimeValue;
                    $succeeded = true;
                }
            } elsif ($commandType eq "SubmitCode") {
                #
                #                                                    SubmitCode
                #
                $code = $command->{'code'};
                $result = eval $code;
                publish({
                    "eventType" => "ReturnValueProduced",
                    "event" => {
                        "formattedValues" => [{
                            "mimeType" => "text/plain",
                            "value" => "$result"
                        }],
                    },
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
                $succeeded = true;
            } else {
                $succeeded = false;
            }

            if ($succeeded) {
                publish({
                    "eventType" => "CommandSucceeded",
                    "event" => {},
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
            } else {
                publish({
                    "eventType" => "CommandFailed",
                    "event" => {
                        "message" => "Unknown command type: $commandType"
                    },
                    "command" => $envelope,
                    "routingSlip" => [
                        $kernelUri
                    ]
                });
            }
        }
        $eventType = $envelope->{'eventType'};
        if ($eventType) {
            # TODO
        }
    } catch {
        print STDERR "error: $_\n";
    }
}

sub publish {
    print encode_json(\%{$_[0]}) . "\n";
}

sub getStringRepresentationOfValueName {
    my $valueName = shift;
    my $mimeType = shift;
    my $rawValue = $main::{$valueName};
    my $formattedValue;
    if ($mimeType eq "application/json") {
        my @asArray = @{getArray($rawValue)};
        my %asHash = %{getHash($rawValue)};
        if (@asArray) {
            $rawValue = \@asArray;
        }
        elsif (%asHash) {
            $rawValue = \%asHash;
        }
        else {
            $rawValue = $$rawValue;
        }
        $formattedValue = encode_json($rawValue);
    }
    else {
        # assume text/plain
        my @asArray = @{getArray($rawValue)};
        my %asHash = %{getHash($rawValue)};
        if (@asArray) {
            $formattedValue = "(" . join(", ", @asArray) . ")";
        }
        elsif (%asHash) {
            $formattedValue = "(" . join(", ", map { "$_ => $asHash{$_}" } keys %asHash) . ")";
        }
        else {
            $formattedValue = "$$rawValue";
        }
    }

    return $formattedValue;
}

sub getArray {
    my $rawValue = shift;
    if (ref($$rawValue) eq "ARRAY") {
        return \@$$rawValue;
    }
    elsif (@$rawValue) {
        return \@$rawValue;
    }

    return undef;
}

sub getHash {
    my $rawValue = shift;
    if (ref($$rawValue) eq "HASH") {
        return \%$$rawValue;
    }
    elsif (%$rawValue) {
        return \%$rawValue;
    }

    return undef;
}
