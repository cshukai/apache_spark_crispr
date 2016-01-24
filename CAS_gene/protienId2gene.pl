#/usr/bin/perl

#use strict;

use warnings;

use Bio::DB::GenBank;

use Bio::SeqIO;

use Bio::SeqFeatureI;



#open my $OUTFILE, '>', "Gb_parser.fasta";

my $gb = new Bio::DB::GenBank; 

# Récupération dans $seq de l'objet Genbank contenant de nombreuses informations

# Gi, Acc, séquence, Annotations, Organisme, Espèce, Genre ...

my $proteinID=$ARGV[0];

my @Acc = $proteinID; #Accession number de la séquence à traiter

my $seq1 = $gb->get_Seq_by_acc(@Acc);

my $Sequence = $seq1->seq();

my $Description = $seq1->desc();

print "Acc = @Acc\nDescription = $Description\n"; #ecrit le num d'accession et sa description associée

my $seqio_object = $seq1;



for my $feat_object ($seqio_object->get_SeqFeatures) {

    #print "\n", $feat_object->primary_tag, "\n";

    for my $tag ($feat_object->get_all_tags){

        print $tag." ";

        for my $value ($feat_object->get_tag_values($tag)){

            print  $value."\n"      

        }

    }

}



foreach $feat ( $seqio_object->get_SeqFeatures() ) {

    if ($feat->primary_tag() eq "CDS"){ # ne s'intéresse qu'aux tags "CDS"

	foreach $tag ( $feat->get_all_tags() ) {    

	    if ($tag eq "locus_tag"){

		print  "\n>the locus: ", join(' ',$feat->get_tag_values($tag)); #ecrit le locus

	    }

	    elsif ($tag eq "product"){

		print  " ", join(' ',$feat->get_tag_values($tag)); #ecrit le nom du CDS

	    }

	    elsif ($tag eq "coded_by"){

		print "\n\nThe corresponding gene acc: ", $feat->get_tag_values($tag);

            #print ">\n\n", join (' ', $feat-> get_tag_values($tag));     #affiche l'acc. du gene



	    }

	}



     #print "\n"

	print " \n\n", $feat->start, "..", $feat->end,"\n\n";;# " on strand ",     $feat->strand, "\n";

	$out = $feat->seq; #out est un Primary::Seq

	$string = $out->seq(); #recupère la séquence

	print $string."\n\n";

    }

}
