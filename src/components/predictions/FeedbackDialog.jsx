import React, { useState } from 'react';
import { civant } from '@/api/civantClient';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Calendar } from '@/components/ui/calendar';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Star, Calendar as CalendarIcon, CheckCircle } from 'lucide-react';
import { format } from 'date-fns';

export default function FeedbackDialog({ prediction, buyerName, open, onOpenChange }) {
    const [published, setPublished] = useState(null);
    const [actualDate, setActualDate] = useState(null);
    const [rating, setRating] = useState(0);
    const [notes, setNotes] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = async () => {
        setSubmitting(true);
        try {
            const user = await civant.auth.me();
            
            await civant.entities.PredictionFeedback.create({
                buyer_name: buyerName,
                predicted_date: prediction.predicted_date,
                actual_published: published === 'yes',
                actual_date: actualDate ? format(actualDate, 'yyyy-MM-dd') : null,
                accuracy_rating: rating,
                feedback_notes: notes,
                prediction_metadata: JSON.stringify(prediction),
                user_email: user.email
            });
            
            setSubmitted(true);
            setTimeout(() => {
                onOpenChange(false);
                setSubmitted(false);
                setPublished(null);
                setActualDate(null);
                setRating(0);
                setNotes('');
            }, 2000);
        } catch (error) {
            console.error('Error submitting feedback:', error);
        } finally {
            setSubmitting(false);
        }
    };

    if (submitted) {
        return (
            <Dialog open={open} onOpenChange={onOpenChange}>
                <DialogContent className="sm:max-w-md">
                    <div className="flex flex-col items-center justify-center py-8">
                        <CheckCircle className="h-16 w-16 text-emerald-600 mb-4" />
                        <h3 className="text-xl font-semibold text-slate-900">Feedback Submitted</h3>
                        <p className="text-slate-500 mt-2">Thank you for helping improve our forecasts!</p>
                    </div>
                </DialogContent>
            </Dialog>
        );
    }

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="sm:max-w-lg">
                <DialogHeader>
                    <DialogTitle>Forecast Feedback</DialogTitle>
                </DialogHeader>
                
                <div className="space-y-6 py-4">
                    {/* Prediction Info */}
                    <div className="p-3 bg-slate-50 rounded-lg text-sm">
                        <p className="font-medium text-slate-700">Estimated Date</p>
                        <p className="text-slate-900">{format(new Date(prediction.predicted_date), 'MMMM d, yyyy')}</p>
                        <p className="text-xs text-slate-500 mt-1">for {buyerName}</p>
                    </div>
                    
                    {/* Was it published? */}
                    <div className="space-y-2">
                        <Label>Was a tender actually published around this date?</Label>
                        <RadioGroup value={published} onValueChange={setPublished}>
                            <div className="flex items-center space-x-2">
                                <RadioGroupItem value="yes" id="yes" />
                                <Label htmlFor="yes" className="font-normal cursor-pointer">Yes, a tender was published</Label>
                            </div>
                            <div className="flex items-center space-x-2">
                                <RadioGroupItem value="no" id="no" />
                                <Label htmlFor="no" className="font-normal cursor-pointer">No, no tender was published</Label>
                            </div>
                        </RadioGroup>
                    </div>
                    
                    {/* Actual date if published */}
                    {published === 'yes' && (
                        <div className="space-y-2">
                            <Label>Actual Publication Date</Label>
                            <Popover>
                                <PopoverTrigger asChild>
                                    <Button variant="outline" className="w-full justify-start text-left">
                                        <CalendarIcon className="mr-2 h-4 w-4" />
                                        {actualDate ? format(actualDate, 'PPP') : 'Select date'}
                                    </Button>
                                </PopoverTrigger>
                                <PopoverContent className="w-auto p-0">
                                    <Calendar mode="single" selected={actualDate} onSelect={setActualDate} />
                                </PopoverContent>
                            </Popover>
                        </div>
                    )}
                    
                    {/* Rating */}
                    <div className="space-y-2">
                        <Label>Forecast Accuracy Rating</Label>
                        <div className="flex gap-2">
                            {[1, 2, 3, 4, 5].map(star => (
                                <button
                                    key={star}
                                    type="button"
                                    onClick={() => setRating(star)}
                                    className="transition-transform hover:scale-110"
                                >
                                    <Star 
                                        className={`h-8 w-8 ${
                                            star <= rating 
                                                ? 'fill-amber-400 text-amber-400' 
                                                : 'text-slate-300'
                                        }`} 
                                    />
                                </button>
                            ))}
                        </div>
                        <p className="text-xs text-slate-500">
                            1 = Very inaccurate, 5 = Very accurate
                        </p>
                    </div>
                    
                    {/* Notes */}
                    <div className="space-y-2">
                        <Label>Additional Comments (Optional)</Label>
                        <Textarea 
                            placeholder="Any insights on why the forecast was accurate/inaccurate?"
                            value={notes}
                            onChange={(e) => setNotes(e.target.value)}
                            rows={3}
                        />
                    </div>
                </div>
                
                <div className="flex gap-2 justify-end">
                    <Button variant="outline" onClick={() => onOpenChange(false)}>
                        Cancel
                    </Button>
                    <Button 
                        onClick={handleSubmit}
                        disabled={!published || !rating || submitting}
                        className="bg-indigo-600 hover:bg-indigo-700"
                    >
                        {submitting ? 'Submitting...' : 'Submit Feedback'}
                    </Button>
                </div>
            </DialogContent>
        </Dialog>
    );
}